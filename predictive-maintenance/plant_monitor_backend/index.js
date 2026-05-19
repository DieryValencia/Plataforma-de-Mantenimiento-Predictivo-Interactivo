/**
 * ============================================================
 *  PLANT MONITOR BACKEND — Dashboard de Telemetría en Tiempo Real
 * ============================================================
 *  Servidor WebSocket (:8081) que consume de Kafka los tópicos
 *  sensor_data, alerts_critical y alerts_warning. Consolida el
 *  estado de la planta en memoria y retransmite cada cambio
 *  a todos los clientes web conectados.
 * ============================================================
 */

"use strict";

const { Kafka } = require("kafkajs");
const { WebSocketServer } = require("ws");

// ── Configuración ──────────────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const WS_PORT = 8081;
const TOPIC_SENSOR_DATA = "sensor_data";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";

// ── Instancia Kafka ────────────────────────────────────────
const kafka = new Kafka({
  clientId: "plant-monitor",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "plant-monitor-group" });

// ── Estado Consolidado de la Planta ────────────────────────
const plantState = new Map(); // sensor_id → { sensor_id, vibration, status, timestamp }

// ── WebSocket Server ───────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });

wss.on("connection", (ws) => {
  console.log("[plant_monitor] 🌐 Cliente WebSocket conectado. Total:", wss.clients.size);

  // Enviar estado actual completo al nuevo cliente
  const snapshot = Array.from(plantState.values());
  ws.send(JSON.stringify({ type: "FULL_STATE", data: snapshot }));

  ws.on("close", () => {
    console.log("[plant_monitor] 🔌 Cliente desconectado. Total:", wss.clients.size);
  });
});

// ── Broadcast a todos los clientes ─────────────────────────
function broadcast(payload) {
  const message = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // WebSocket.OPEN
      client.send(message);
    }
  });
}

// ── Reintento resiliente ───────────────────────────────────
async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      await connectFn();
      connected = true;
      console.log(`[plant_monitor] ✅ ${label} conectado.`);
    } catch (err) {
      console.error(`[plant_monitor] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// ── Procesar Mensajes ──────────────────────────────────────
function processMessage(topic, payload) {
  const sensorId = payload.sensor_id;

  if (topic === TOPIC_SENSOR_DATA) {
    // Telemetría normal: solo marcar OK si la vibración ≤ 75
    const currentState = plantState.get(sensorId);
    const status = payload.vibration <= 75 ? "OK" : (currentState ? currentState.status : "OK");

    plantState.set(sensorId, {
      sensor_id: sensorId,
      vibration: payload.vibration,
      status: payload.vibration <= 75 ? "OK" : status,
      unit: payload.unit || "mm/s",
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "SENSOR_UPDATE",
      data: plantState.get(sensorId),
    });

  } else if (topic === TOPIC_ALERTS_CRITICAL) {
    plantState.set(sensorId, {
      sensor_id: sensorId,
      vibration: payload.vibration,
      status: "CRITICAL",
      unit: "mm/s",
      message: payload.message,
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "ALERT_CRITICAL",
      data: plantState.get(sensorId),
    });

  } else if (topic === TOPIC_ALERTS_WARNING) {
    plantState.set(sensorId, {
      sensor_id: sensorId,
      vibration: payload.avg_vibration,
      status: "WARNING",
      unit: "mm/s",
      message: payload.message,
      readings: payload.readings,
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "ALERT_WARNING",
      data: plantState.get(sensorId),
    });
  }
}

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("═══════════════════════════════════════════════════════════════");
  console.log("  PLANT MONITOR BACKEND — WebSocket :8081 (Telemetría)        ");
  console.log("═══════════════════════════════════════════════════════════════");

  await connectWithRetry("Kafka Consumer", () => consumer.connect());

  await consumer.subscribe({
    topics: [TOPIC_SENSOR_DATA, TOPIC_ALERTS_CRITICAL, TOPIC_ALERTS_WARNING],
    fromBeginning: false,
  });

  console.log("[plant_monitor] 👂 Escuchando tópicos de telemetría y alertas…");
  console.log(`[plant_monitor] 🌐 WebSocket Server en puerto ${WS_PORT}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        processMessage(topic, payload);
      } catch (err) {
        console.error("[plant_monitor] ❌ Error procesando mensaje:", err.message);
      }
    },
  });
})();
