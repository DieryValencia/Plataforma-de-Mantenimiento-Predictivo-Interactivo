/**
 * ============================================================
 *  ALERT ROUTER — Puente Kafka → RabbitMQ (Interoperabilidad)
 * ============================================================
 *  Consume de los tópicos `alerts_critical` y `alerts_warning`,
 *  enriquece cada alerta con opciones aleatorias de decisión humana
 *  y publica al Exchange Fanout `human_alerts` de RabbitMQ.
 *
 *  Formato RabbitMQ: { alert_id, sensor_id, type, options, ... }
 * ============================================================
 */

"use strict";

const crypto = require("crypto");
const { Kafka } = require("kafkajs");
const amqp = require("amqplib");

// ── Configuración ──────────────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";
const RABBIT_EXCHANGE = "human_alerts";

const CRITICAL_OPTIONS_POOL = ["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"];
const WARNING_OPTIONS_POOL = ["PROGRAMAR_MANTENIMIENTO_AHORA", "RECONOCER_Y_ESPERAR_24H"];

// ── Instancia Kafka ────────────────────────────────────────
const kafka = new Kafka({
  clientId: "alert-router",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "alert-router-group" });

let rabbitChannel = null;

async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      await connectFn();
      connected = true;
      console.log(`[alert_router] ✅ ${label} conectado.`);
    } catch (err) {
      console.error(`[alert_router] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

function toSensorCode(sensorId) {
  if (!sensorId) return "UNKNOWN";
  const match = String(sensorId).match(/sensor_([A-J])/i);
  if (match) return match[1].toUpperCase();
  return String(sensorId).replace(/^sensor_/i, "").toUpperCase();
}

/** Opciones fijas del ejercicio (orden estable, sin aleatoriedad) */
function buildOptions(pool) {
  return [...pool];
}

async function setupRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await connection.createChannel();
  await rabbitChannel.assertExchange(RABBIT_EXCHANGE, "fanout", { durable: true });
  console.log(`[alert_router] 📢 Exchange Fanout '${RABBIT_EXCHANGE}' declarado.`);
}

function buildHumanAlertMessage(alertPayload, sourceTopic) {
  const type = sourceTopic === TOPIC_ALERTS_CRITICAL ? "CRITICAL" : "WARNING";
  const optionsPool =
    type === "CRITICAL" ? CRITICAL_OPTIONS_POOL : WARNING_OPTIONS_POOL;

  return {
    alert_id: crypto.randomUUID(),
    sensor_id: alertPayload.sensor_code || toSensorCode(alertPayload.sensor_id),
    type,
    options: buildOptions(optionsPool),
    message: alertPayload.message,
    vibration: alertPayload.vibration ?? alertPayload.avg_vibration,
    source_topic: sourceTopic,
    detected_at: alertPayload.detected_at,
    routed_at: new Date().toISOString(),
  };
}

function publishToRabbit(humanAlert) {
  if (!rabbitChannel) {
    console.error("[alert_router] ❌ Canal RabbitMQ no disponible.");
    return;
  }

  const buffer = Buffer.from(JSON.stringify(humanAlert));
  rabbitChannel.publish(RABBIT_EXCHANGE, "", buffer, {
    persistent: true,
    contentType: "application/json",
  });

  const level = humanAlert.type === "CRITICAL" ? "🔴" : "🟡";
  console.log(
    `[alert_router] ${level} → RabbitMQ | alert_id: ${humanAlert.alert_id} | sensor: ${humanAlert.sensor_id} | opciones: [${humanAlert.options.join(", ")}]`
  );
}

(async () => {
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  ALERT ROUTER — Puente Kafka → RabbitMQ                   ");
  console.log("═══════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupRabbitMQ);
  await connectWithRetry("Kafka Consumer", () => consumer.connect());

  await consumer.subscribe({
    topics: [TOPIC_ALERTS_CRITICAL, TOPIC_ALERTS_WARNING],
    fromBeginning: false,
  });

  console.log("[alert_router] 👂 Escuchando tópicos:", TOPIC_ALERTS_CRITICAL, "&", TOPIC_ALERTS_WARNING);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const alertPayload = JSON.parse(message.value.toString());
        const humanAlert = buildHumanAlertMessage(alertPayload, topic);
        publishToRabbit(humanAlert);
      } catch (err) {
        console.error("[alert_router] ❌ Error procesando alerta:", err.message);
      }
    },
  });
})();
