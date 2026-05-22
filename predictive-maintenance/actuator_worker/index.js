/**
 * ============================================================
 *  ACTUATOR WORKER — Ejecutor de Acciones Críticas
 * ============================================================
 *  Consume de `critical_actions_queue` vinculada al Exchange
 *  Direct `actions_direct` (routing key: critical).
 * ============================================================
 */

"use strict";

const amqp = require("amqplib");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const ACTIONS_DIRECT = "actions_direct";
const RK_CRITICAL = "critical";
const CRITICAL_QUEUE = "critical_actions_queue";

async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      const result = await connectFn();
      connected = true;
      console.log(`[actuator_worker] ✅ ${label} conectado.`);
      return result;
    } catch (err) {
      console.error(`[actuator_worker] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

function simulateShutdown(payload) {
  console.log("");
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log("║  ⚡⚡⚡  CORTE DE ENERGÍA — ACTUADOR INDUSTRIAL  ⚡⚡⚡       ║");
  console.log("╠══════════════════════════════════════════════════════════════╣");
  console.log(`║  🆔 Alert ID:    ${payload.alert_id || "N/A"}`.padEnd(63) + "║");
  console.log(`║  🏭 Sensor:      ${payload.sensor_id || "N/A"}`.padEnd(63) + "║");
  console.log(`║  🔴 Acción:      ${payload.chosen_action || "APAGADO_INMEDIATO"}`.padEnd(63) + "║");
  console.log(`║  ⏰ Timestamp:   ${payload.dispatched_at || new Date().toISOString()}`.padEnd(63) + "║");
  console.log(`║  📊 Vibración:   ${payload.vibration || "N/A"} mm/s`.padEnd(63) + "║");
  console.log("║                                                             ║");
  console.log("║  ✅ ACTUADOR APAGADO EXITOSAMENTE                           ║");
  console.log("║  🔒 Bloqueo de seguridad activado                           ║");
  console.log("║  📋 Ticket de incidente generado                            ║");
  console.log("╚══════════════════════════════════════════════════════════════╝");
  console.log("");
}

async function setupConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange(ACTIONS_DIRECT, "direct", { durable: true });
  await channel.assertQueue(CRITICAL_QUEUE, { durable: true });
  await channel.bindQueue(CRITICAL_QUEUE, ACTIONS_DIRECT, RK_CRITICAL);
  channel.prefetch(1);

  console.log(`[actuator_worker] 👂 Esperando en '${CRITICAL_QUEUE}' ← ${ACTIONS_DIRECT}[${RK_CRITICAL}]`);

  channel.consume(CRITICAL_QUEUE, (msg) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      simulateShutdown(payload);
      channel.ack(msg);
    } catch (err) {
      console.error("[actuator_worker] ❌ Error procesando mensaje:", err.message);
      channel.nack(msg, false, false);
    }
  });

  return connection;
}

(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  ACTUATOR WORKER — Ejecutor de Acciones Críticas            ");
  console.log("══════════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupConsumer);
})();
