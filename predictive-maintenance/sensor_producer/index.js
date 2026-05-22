/**
 * ============================================================
 *  SENSOR PRODUCER — Telemetría + Control por decisiones humanas
 * ============================================================
 *  Publica lecturas en `sensor_data` solo para sensores ACTIVE.
 *  Estados: ACTIVE | SHUTDOWN | COOLDOWN | MAINTENANCE
 *  API POST /control — aplicada por action_dispatcher
 * ============================================================
 */

"use strict";

const http = require("http");
const { Kafka } = require("kafkajs");

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const CONTROL_PORT = Number(process.env.CONTROL_PORT) || 3002;
const TOPIC_SENSOR_DATA = "sensor_data";
const TOPIC_SENSOR_STATUS = "sensor_status";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";

const SENSOR_IDS = [
  "sensor_A", "sensor_B", "sensor_C", "sensor_D", "sensor_E",
  "sensor_F", "sensor_G", "sensor_H", "sensor_I", "sensor_J",
];

const PUBLISH_INTERVAL_MS = 500;
const MAINTENANCE_DURATION_MS = Number(process.env.MAINTENANCE_DURATION_MS) || 45000;
const DELAY_10MIN_MS = Number(process.env.DELAY_10MIN_MS) || 600000;
const DELAY_24H_MS = Number(process.env.DELAY_24H_MS) || 86400000;
/** Una sola alerta crítica (>90) en toda la planta por intervalo de simulación (default 5 min) */
const CRITICAL_INTERVAL_MS = Number(process.env.CRITICAL_INTERVAL_MS) || 5 * 60 * 1000;

let nextCriticalAllowedAt = Date.now() + 15000;
let warningBurstSensor = null;
let warningBurstRemaining = 0;

const MODES = {
  ACTIVE: "ACTIVE",
  SHUTDOWN: "SHUTDOWN",
  COOLDOWN: "COOLDOWN",
  MAINTENANCE: "MAINTENANCE",
};

const kafka = new Kafka({
  clientId: "sensor-producer",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const admin = kafka.admin();
const producer = kafka.producer();

/** @type {Map<string, object>} */
const sensorState = new Map();

function toFullSensorId(id) {
  if (!id) return null;
  const s = String(id).trim();
  if (s.startsWith("sensor_")) return s;
  return `sensor_${s.replace(/^sensor_/i, "").toUpperCase()}`;
}

function toSensorCode(sensorId) {
  const m = String(sensorId).match(/sensor_([A-J])/i);
  return m ? m[1].toUpperCase() : sensorId;
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Telemetría controlada (no aleatoria pura):
 * - Normal: 42–74 mm/s
 * - Crítica: >90 como máximo 1 vez cada CRITICAL_INTERVAL_MS (planta)
 * - Advertencia: ráfaga de 3 lecturas 76–78 en el mismo sensor
 */
function generateControlledVibration(sensorId) {
  const now = Date.now();

  if (now >= nextCriticalAllowedAt) {
    nextCriticalAllowedAt = now + CRITICAL_INTERVAL_MS;
    warningBurstSensor = null;
    warningBurstRemaining = 0;
    const v = randomInt(91, 98);
    const nextMin = Math.round(CRITICAL_INTERVAL_MS / 60000);
    console.log(
      `[sensor_producer] 🔴 Lectura CRÍTICA programada → ${sensorId} ${v} mm/s (próxima en ${nextMin} min)`
    );
    return v;
  }

  if (warningBurstRemaining > 0 && warningBurstSensor === sensorId) {
    warningBurstRemaining -= 1;
    return randomInt(76, 78);
  }

  if (!warningBurstRemaining && Math.random() < 0.04) {
    warningBurstSensor = sensorId;
    warningBurstRemaining = 2;
    return randomInt(76, 78);
  }

  return randomInt(42, 74);
}

function initSensorStates() {
  SENSOR_IDS.forEach((id) => {
    sensorState.set(id, {
      mode: MODES.ACTIVE,
      lastVibration: randomInt(45, 70),
      cooldownUntil: null,
      cooldownTotal: 0,
      maintenanceUntil: null,
      lastAction: null,
      reactivateTimer: null,
    });
  });
}

async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      await connectFn();
      connected = true;
      console.log(`[sensor_producer] ✅ ${label} conectado.`);
    } catch (err) {
      console.error(`[sensor_producer] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

async function ensureTopics() {
  const topics = [
    { topic: TOPIC_SENSOR_DATA, numPartitions: 3, replicationFactor: 1 },
    { topic: TOPIC_SENSOR_STATUS, numPartitions: 3, replicationFactor: 1 },
    { topic: TOPIC_ALERTS_CRITICAL, numPartitions: 3, replicationFactor: 1 },
    { topic: TOPIC_ALERTS_WARNING, numPartitions: 3, replicationFactor: 1 },
  ];
  try {
    await admin.createTopics({ topics });
    console.log("[sensor_producer] 📦 Tópicos verificados.");
  } catch (err) {
    console.log("[sensor_producer] ℹ️  Tópicos:", err.message);
  }
}

function clearReactivateTimer(st) {
  if (st.reactivateTimer) {
    clearTimeout(st.reactivateTimer);
    st.reactivateTimer = null;
  }
}

function scheduleModeEnd(sensorId, ms, nextMode = MODES.ACTIVE) {
  const st = sensorState.get(sensorId);
  if (!st) return;
  clearReactivateTimer(st);
  st.reactivateTimer = setTimeout(() => {
    st.mode = nextMode;
    st.cooldownUntil = null;
    st.cooldownTotal = 0;
    st.maintenanceUntil = null;
    st.lastVibration = randomInt(42, 68);
    console.log(`[sensor_producer] ♻️  ${sensorId} → ${nextMode}`);
    publishStatus(sensorId, `Sensor reactivado tras temporizador`);
  }, ms);
}

async function publishStatus(sensorId, message) {
  const st = sensorState.get(sensorId);
  if (!st) return;

  let status = "OK";
  let cooldownRemainingMs = 0;
  let cooldownPct = 0;

  if (st.mode === MODES.SHUTDOWN) {
    status = "SHUTDOWN";
  } else if (st.mode === MODES.MAINTENANCE) {
    status = "MAINTENANCE";
    const rem = Math.max(0, (st.maintenanceUntil || 0) - Date.now());
    cooldownRemainingMs = rem;
    cooldownPct = st.maintenanceTotal ? (rem / st.maintenanceTotal) * 100 : 0;
    if (rem <= 0) {
      st.mode = MODES.ACTIVE;
      status = "OK";
      st.maintenanceUntil = null;
      message = message || "Mantenimiento completado";
    }
  } else if (st.mode === MODES.COOLDOWN) {
    status = "COOLDOWN";
    cooldownRemainingMs = Math.max(0, (st.cooldownUntil || 0) - Date.now());
    cooldownPct = st.cooldownTotal
      ? Math.min(100, (cooldownRemainingMs / st.cooldownTotal) * 100)
      : 0;
    if (cooldownRemainingMs <= 0) {
      st.mode = MODES.ACTIVE;
      status = "OK";
      st.cooldownUntil = null;
      message = message || "Cooldown finalizado — sensor activo";
    }
  }

  const payload = {
    sensor_id: sensorId,
    sensor_code: toSensorCode(sensorId),
    vibration: st.mode === MODES.ACTIVE ? st.lastVibration : 0,
    status,
    unit: "mm/s",
    timestamp: new Date().toISOString(),
    control_mode: st.mode,
    last_action: st.lastAction,
    message: message || null,
    cooldown_remaining_ms: cooldownRemainingMs,
    cooldown_pct: Math.round(cooldownPct),
  };

  try {
    await producer.send({
      topic: TOPIC_SENSOR_STATUS,
      messages: [{ key: sensorId, value: JSON.stringify(payload) }],
    });
  } catch (err) {
    console.error("[sensor_producer] ❌ Error sensor_status:", err.message);
  }
}

function applyControl(body) {
  const sensorId = toFullSensorId(body.sensor_id);
  if (!sensorId || !sensorState.has(sensorId)) {
    throw new Error(`Sensor desconocido: ${body.sensor_id}`);
  }

  const action = body.chosen_action;
  const st = sensorState.get(sensorId);
  clearReactivateTimer(st);
  st.lastAction = action;

  switch (action) {
    case "APAGADO_INMEDIATO":
      st.mode = MODES.SHUTDOWN;
      st.cooldownUntil = null;
      st.maintenanceUntil = null;
      console.log(`[sensor_producer] ⛔ ${sensorId} APAGADO — sin más lecturas`);
      publishStatus(sensorId, "Apagado por operador — actuador crítico");
      return { sensor_id: sensorId, mode: MODES.SHUTDOWN };

    case "IGNORAR_10_MINUTOS": {
      const ms = Number(body.duration_ms) || DELAY_10MIN_MS;
      st.mode = MODES.COOLDOWN;
      st.cooldownTotal = ms;
      st.cooldownUntil = Date.now() + ms;
      scheduleModeEnd(sensorId, ms);
      console.log(`[sensor_producer] ⏳ ${sensorId} COOLDOWN ${ms}ms (ignorar)`);
      publishStatus(sensorId, `Ignorado — reactivación en ${Math.round(ms / 1000)}s`);
      return { sensor_id: sensorId, mode: MODES.COOLDOWN, duration_ms: ms };
    }

    case "RECONOCER_Y_ESPERAR_24H": {
      const ms = Number(body.duration_ms) || DELAY_24H_MS;
      st.mode = MODES.COOLDOWN;
      st.cooldownTotal = ms;
      st.cooldownUntil = Date.now() + ms;
      scheduleModeEnd(sensorId, ms);
      console.log(`[sensor_producer] ⏳ ${sensorId} COOLDOWN ${ms}ms (24h)`);
      publishStatus(sensorId, `En espera — reactivación programada`);
      return { sensor_id: sensorId, mode: MODES.COOLDOWN, duration_ms: ms };
    }

    case "PROGRAMAR_MANTENIMIENTO_AHORA": {
      const ms = Number(body.duration_ms) || MAINTENANCE_DURATION_MS;
      st.mode = MODES.MAINTENANCE;
      st.maintenanceTotal = ms;
      st.maintenanceUntil = Date.now() + ms;
      scheduleModeEnd(sensorId, ms);
      console.log(`[sensor_producer] 🔧 ${sensorId} MANTENIMIENTO ${ms}ms`);
      publishStatus(sensorId, "Mantenimiento en curso — lecturas pausadas");
      return { sensor_id: sensorId, mode: MODES.MAINTENANCE, duration_ms: ms };
    }

    default:
      throw new Error(`Acción no soportada: ${action}`);
  }
}

function getActiveSensors() {
  return SENSOR_IDS.filter((id) => sensorState.get(id).mode === MODES.ACTIVE);
}

async function publishTelemetryTick() {
  for (const sensorId of SENSOR_IDS) {
    const st = sensorState.get(sensorId);

    if (st.mode === MODES.COOLDOWN || st.mode === MODES.MAINTENANCE) {
      await publishStatus(sensorId);
      continue;
    }

    if (st.mode === MODES.SHUTDOWN) {
      st.statusTick = (st.statusTick || 0) + 1;
      if (st.statusTick % 10 === 0) await publishStatus(sensorId);
      continue;
    }
  }

  const active = getActiveSensors();
  if (active.length === 0) return;

  const sensorId = active[randomInt(0, active.length - 1)];
  const st = sensorState.get(sensorId);
  st.lastVibration = generateControlledVibration(sensorId);

  const payload = {
    sensor_id: sensorId,
    sensor_code: toSensorCode(sensorId),
    vibration: st.lastVibration,
    unit: "mm/s",
    timestamp: new Date().toISOString(),
  };

  try {
    await producer.send({
      topic: TOPIC_SENSOR_DATA,
      messages: [{ key: sensorId, value: JSON.stringify(payload) }],
    });
    console.log(`[sensor_producer] 📡 ${sensorId} → ${payload.vibration} mm/s`);
  } catch (err) {
    console.error("[sensor_producer] ❌ Error publicación:", err.message);
  }
}

function startPublishing() {
  console.log(`[sensor_producer] 🚀 Telemetría cada ${PUBLISH_INTERVAL_MS}ms`);
  setInterval(() => publishTelemetryTick(), PUBLISH_INTERVAL_MS);
}

function startControlServer() {
  const server = http.createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          status: "healthy",
          sensors: Object.fromEntries(
            [...sensorState.entries()].map(([k, v]) => [k, { mode: v.mode, lastAction: v.lastAction }])
          ),
        })
      );
      return;
    }

    if (req.method === "POST" && req.url === "/control") {
      let data = "";
      req.on("data", (c) => (data += c));
      req.on("end", () => {
        try {
          const body = JSON.parse(data);
          const result = applyControl(body);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "ACK", ...result }));
        } catch (err) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "NACK", error: err.message }));
        }
      });
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not found" }));
  });

  server.listen(CONTROL_PORT, () => {
    console.log(`[sensor_producer] 🎛️  Control API :${CONTROL_PORT} POST /control`);
  });
}

(async () => {
  console.log("═══════════════════════════════════════════════════");
  console.log("  SENSOR PRODUCER — Telemetría + Control Operador   ");
  console.log("═══════════════════════════════════════════════════");

  initSensorStates();
  await connectWithRetry("Kafka Admin", () => admin.connect());
  await ensureTopics();
  await admin.disconnect();
  await connectWithRetry("Kafka Producer", () => producer.connect());

  for (const id of SENSOR_IDS) {
    await publishStatus(id, "Estado inicial");
  }

  const intervalMin = Math.round(CRITICAL_INTERVAL_MS / 60000);
  console.log(`[sensor_producer] ⏱️  Ventana crítica: 1 alerta cada ${intervalMin} min de simulación`);

  startControlServer();
  startPublishing();
})();
