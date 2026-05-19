/**
 * ============================================================
 *  ALERT ROUTER — Puente Kafka → RabbitMQ (Interoperabilidad)
 * ============================================================
 *  Consume de los tópicos `alerts_critical` y `alerts_warning`,
 *  enriquece cada alerta con opciones de decisión humana y
 *  publica al Exchange Fanout `human_alerts` de RabbitMQ.
 * ============================================================
 */

"use strict";

const { Kafka } = require("kafkajs");
const amqp = require("amqplib");

// ── Configuración ──────────────────────────────────────────
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";
const RABBIT_EXCHANGE = "human_alerts";

// ── Instancia Kafka ────────────────────────────────────────
const kafka = new Kafka({
  clientId: "alert-router",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "alert-router-group" });

// ── Estado RabbitMQ ────────────────────────────────────────
let rabbitChannel = null;

// ── Reintento resiliente ───────────────────────────────────
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

// ── Conexión a RabbitMQ ────────────────────────────────────
async function setupRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await connection.createChannel();

  // Declarar el Exchange Fanout para distribución a todas las colas suscritas
  await rabbitChannel.assertExchange(RABBIT_EXCHANGE, "fanout", { durable: true });
  console.log(`[alert_router] 📢 Exchange Fanout '${RABBIT_EXCHANGE}' declarado.`);
}

// ── Enriquecimiento de Opciones ────────────────────────────
function enrichPayload(alertPayload, sourceTopic) {
  let options = [];

  if (sourceTopic === TOPIC_ALERTS_CRITICAL) {
    options = ["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"];
  } else if (sourceTopic === TOPIC_ALERTS_WARNING) {
    options = ["PROGRAMAR_MANTENIMIENTO_AHORA", "RECONOCER_Y_ESPERAR_24H"];
  }

  return {
    ...alertPayload,
    source_topic: sourceTopic,
    options: options,
    routed_at: new Date().toISOString(),
  };
}

// ── Publicar a RabbitMQ ────────────────────────────────────
function publishToRabbit(enrichedPayload) {
  if (!rabbitChannel) {
    console.error("[alert_router] ❌ Canal RabbitMQ no disponible.");
    return;
  }

  const buffer = Buffer.from(JSON.stringify(enrichedPayload));
  rabbitChannel.publish(RABBIT_EXCHANGE, "", buffer, {
    persistent: true,
    contentType: "application/json",
  });

  const level = enrichedPayload.alert_type === "CRITICAL" ? "🔴" : "🟡";
  console.log(
    `[alert_router] ${level} → RabbitMQ | sensor: ${enrichedPayload.sensor_id} | opciones: [${enrichedPayload.options.join(", ")}]`
  );
}

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  ALERT ROUTER — Puente Kafka → RabbitMQ                   ");
  console.log("═══════════════════════════════════════════════════════════");

  // Conectar a RabbitMQ con reintentos
  await connectWithRetry("RabbitMQ", setupRabbitMQ);

  // Conectar al consumidor Kafka con reintentos
  await connectWithRetry("Kafka Consumer", () => consumer.connect());

  // Suscribirse a ambos tópicos de alertas
  await consumer.subscribe({ topics: [TOPIC_ALERTS_CRITICAL, TOPIC_ALERTS_WARNING], fromBeginning: false });

  console.log("[alert_router] 👂 Escuchando tópicos:", TOPIC_ALERTS_CRITICAL, "&", TOPIC_ALERTS_WARNING);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const alertPayload = JSON.parse(message.value.toString());
        const enriched = enrichPayload(alertPayload, topic);
        publishToRabbit(enriched);
      } catch (err) {
        console.error("[alert_router] ❌ Error procesando alerta:", err.message);
      }
    },
  });
})();
