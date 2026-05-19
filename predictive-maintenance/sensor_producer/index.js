/**
 * ============================================================
 *  SENSOR PRODUCER — Productor de Telemetría Industrial (Kafka)
 * ============================================================
 *  Simula 10 sensores de vibración (sensor_A … sensor_J) y
 *  publica lecturas cada 500 ms al tópico `sensor_data`.
 *  Utiliza el sensor_id como KEY para garantizar orden por
 *  partición (afinidad de clave).
 * ============================================================
 */

"use strict";

const { Kafka } = require("kafkajs");

// ── Configuración ──────────────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const TOPIC_SENSOR_DATA = "sensor_data";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";
const SENSOR_IDS = [
  "sensor_A", "sensor_B", "sensor_C", "sensor_D", "sensor_E",
  "sensor_F", "sensor_G", "sensor_H", "sensor_I", "sensor_J",
];
const PUBLISH_INTERVAL_MS = 500;

// ── Instancia Kafka ────────────────────────────────────────
const kafka = new Kafka({
  clientId: "sensor-producer",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const admin = kafka.admin();
const producer = kafka.producer();

// ── Utilidades ─────────────────────────────────────────────
function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomSensor() {
  return SENSOR_IDS[randomInt(0, SENSOR_IDS.length - 1)];
}

function buildPayload(sensorId) {
  return {
    sensor_id: sensorId,
    vibration: randomInt(40, 100),
    unit: "mm/s",
    timestamp: new Date().toISOString(),
  };
}

// ── Bucle de reintento para conexión resiliente ────────────
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

// ── Asegurar tópicos ───────────────────────────────────────
async function ensureTopics() {
  const topics = [
    { topic: TOPIC_SENSOR_DATA, numPartitions: 3, replicationFactor: 1 },
    { topic: TOPIC_ALERTS_CRITICAL, numPartitions: 3, replicationFactor: 1 },
    { topic: TOPIC_ALERTS_WARNING, numPartitions: 3, replicationFactor: 1 },
  ];

  try {
    await admin.createTopics({ topics });
    console.log("[sensor_producer] 📦 Tópicos creados/verificados:", topics.map((t) => t.topic).join(", "));
  } catch (err) {
    // Los tópicos pueden existir ya — no es un error fatal
    console.log("[sensor_producer] ℹ️  Tópicos ya existentes o auto-creados:", err.message);
  }
}

// ── Publicación continua ───────────────────────────────────
async function startPublishing() {
  console.log("[sensor_producer] 🚀 Iniciando publicación cada", PUBLISH_INTERVAL_MS, "ms");

  setInterval(async () => {
    const sensorId = randomSensor();
    const payload = buildPayload(sensorId);

    try {
      await producer.send({
        topic: TOPIC_SENSOR_DATA,
        messages: [
          {
            key: sensorId,                        // Afinidad de partición
            value: JSON.stringify(payload),
          },
        ],
      });
      console.log(
        `[sensor_producer] 📡 ${sensorId} → vibración: ${payload.vibration} mm/s`
      );
    } catch (err) {
      console.error("[sensor_producer] ❌ Error de publicación:", err.message);
    }
  }, PUBLISH_INTERVAL_MS);
}

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("═══════════════════════════════════════════════════");
  console.log("  SENSOR PRODUCER — Telemetría Industrial (Kafka)  ");
  console.log("═══════════════════════════════════════════════════");

  await connectWithRetry("Kafka Admin", () => admin.connect());
  await ensureTopics();
  await admin.disconnect();

  await connectWithRetry("Kafka Producer", () => producer.connect());
  await startPublishing();
})();
