/**
 * ============================================================
 *  PLANT MONITOR BACKEND — Dashboard de Telemetría en Tiempo Real
 * ============================================================
 *  Servidor WebSocket (:8081) que consume de Kafka los tópicos
 *  sensor_status, alerts_critical y alerts_warning. Consolida el
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
const TOPIC_SENSOR_STATUS = "sensor_status";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";

const kafka = new Kafka({
  clientId: "plant-monitor",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "plant-monitor-group" });
const plantState = new Map();

const wss = new WebSocketServer({ port: WS_PORT });

wss.on("connection", (ws) => {
  console.log("[plant_monitor] 🌐 Cliente WebSocket conectado. Total:", wss.clients.size);
  const snapshot = Array.from(plantState.values());
  ws.send(JSON.stringify({ type: "FULL_STATE", data: snapshot }));

  ws.on("close", () => {
    console.log("[plant_monitor] 🔌 Cliente desconectado. Total:", wss.clients.size);
  });
});

function broadcast(payload) {
  const message = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(message);
    }
  });
}

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

function processMessage(topic, payload) {
  const sensorKey = payload.sensor_id;
  const displayId = payload.sensor_code
    ? `sensor_${payload.sensor_code}`
    : sensorKey;

  if (topic === TOPIC_SENSOR_STATUS) {
    plantState.set(sensorKey, {
      sensor_id: displayId,
      sensor_code: payload.sensor_code || payload.sensor_id,
      vibration: payload.vibration,
      status: payload.status,
      unit: payload.unit || "mm/s",
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "SENSOR_UPDATE",
      data: plantState.get(sensorKey),
    });
  } else if (topic === TOPIC_ALERTS_CRITICAL) {
    plantState.set(sensorKey, {
      sensor_id: displayId,
      sensor_code: payload.sensor_code || payload.sensor_id,
      vibration: payload.vibration,
      status: "CRITICAL",
      unit: "mm/s",
      message: payload.message,
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "ALERT_CRITICAL",
      data: plantState.get(sensorKey),
    });
  } else if (topic === TOPIC_ALERTS_WARNING) {
    plantState.set(sensorKey, {
      sensor_id: displayId,
      sensor_code: payload.sensor_code || payload.sensor_id,
      vibration: payload.avg_vibration,
      status: "WARNING",
      unit: "mm/s",
      message: payload.message,
      readings: payload.readings,
      timestamp: payload.timestamp,
    });

    broadcast({
      type: "ALERT_WARNING",
      data: plantState.get(sensorKey),
    });
  }
}

(async () => {
  console.log("═══════════════════════════════════════════════════════════════");
  console.log("  PLANT MONITOR BACKEND — WebSocket :8081 (Telemetría)        ");
  console.log("═══════════════════════════════════════════════════════════════");

  await connectWithRetry("Kafka Consumer", () => consumer.connect());

  await consumer.subscribe({
    topics: [TOPIC_SENSOR_STATUS, TOPIC_ALERTS_CRITICAL, TOPIC_ALERTS_WARNING],
    fromBeginning: false,
  });

  console.log(
    "[plant_monitor] 👂 Escuchando:",
    TOPIC_SENSOR_STATUS,
    TOPIC_ALERTS_CRITICAL,
    TOPIC_ALERTS_WARNING
  );
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
