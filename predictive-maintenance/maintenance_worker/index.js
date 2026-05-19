/**
 * ============================================================
 *  MAINTENANCE WORKER — Procesador de Órdenes de Mantenimiento
 * ============================================================
 *  Consume de `maintenance_queue` y procesa tanto las órdenes
 *  inmediatas (PROGRAMAR_MANTENIMIENTO_AHORA) como las
 *  diferidas (RECONOCER_Y_ESPERAR_24H → delayed 15s).
 * ============================================================
 */

"use strict";

const amqp = require("amqplib");

// ── Configuración ──────────────────────────────────────────
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const MAINTENANCE_QUEUE = "maintenance_queue";

// ── Reintento resiliente ───────────────────────────────────
async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      const result = await connectFn();
      connected = true;
      console.log(`[maintenance_worker] ✅ ${label} conectado.`);
      return result;
    } catch (err) {
      console.error(`[maintenance_worker] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// ── Simulación de Orden de Mantenimiento ───────────────────
function processMaintenanceOrder(payload) {
  const isDelayed = payload.chosen_action === "RECONOCER_Y_ESPERAR_24H";
  const tipo = isDelayed ? "DIFERIDA (Delayed 15s)" : "INMEDIATA";

  console.log("");
  console.log("┌──────────────────────────────────────────────────────────────┐");
  console.log("│  🔧🔧🔧  ORDEN DE MANTENIMIENTO PROCESADA  🔧🔧🔧          │");
  console.log("├──────────────────────────────────────────────────────────────┤");
  console.log(`│  📋 Tipo:        ${tipo}`.padEnd(63) + "│");
  console.log(`│  🏭 Sensor:      ${payload.sensor_id || "N/A"}`.padEnd(63) + "│");
  console.log(`│  🎯 Acción:      ${payload.chosen_action || "N/A"}`.padEnd(63) + "│");
  console.log(`│  ⏰ Despachado:  ${payload.dispatched_at || "N/A"}`.padEnd(63) + "│");
  console.log(`│  🕐 Procesado:   ${new Date().toISOString()}`.padEnd(63) + "│");

  if (isDelayed) {
    console.log("│                                                             │");
    console.log("│  ⏳ Este mensaje fue retenido por el Delayed Exchange de     │");
    console.log("│     RabbitMQ durante 15 segundos antes de ser liberado.      │");
  }

  console.log("│                                                             │");
  console.log("│  ✅ Equipo de mantenimiento notificado                       │");
  console.log("│  📝 Orden de trabajo #MT-" + Date.now().toString().slice(-6) + " creada".padEnd(36) + "│");
  console.log("│  📅 Programada en el calendario de planta                    │");
  console.log("└──────────────────────────────────────────────────────────────┘");
  console.log("");
}

// ── Configuración RabbitMQ + Consumo ───────────────────────
async function setupConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertQueue(MAINTENANCE_QUEUE, { durable: true });
  channel.prefetch(1);

  console.log(`[maintenance_worker] 👂 Esperando mensajes en '${MAINTENANCE_QUEUE}'…`);

  channel.consume(MAINTENANCE_QUEUE, (msg) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      processMaintenanceOrder(payload);
      channel.ack(msg);
    } catch (err) {
      console.error("[maintenance_worker] ❌ Error procesando mensaje:", err.message);
      channel.nack(msg, false, false);
    }
  });

  return connection;
}

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  MAINTENANCE WORKER — Órdenes de Mantenimiento              ");
  console.log("══════════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupConsumer);
})();
