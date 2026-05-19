/**
 * ============================================================
 *  ACTUATOR WORKER — Ejecutor de Acciones Críticas
 * ============================================================
 *  Consume de `critical_actions_queue` y simula con logs de
 *  alta visibilidad el corte de energía del actuador industrial.
 * ============================================================
 */

"use strict";

const amqp = require("amqplib");

// ── Configuración ──────────────────────────────────────────
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const CRITICAL_QUEUE = "critical_actions_queue";

// ── Reintento resiliente ───────────────────────────────────
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

// ── Simulación de Corte de Energía ─────────────────────────
function simulateShutdown(payload) {
  console.log("");
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log("║  ⚡⚡⚡  CORTE DE ENERGÍA — ACTUADOR INDUSTRIAL  ⚡⚡⚡       ║");
  console.log("╠══════════════════════════════════════════════════════════════╣");
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

// ── Configuración RabbitMQ + Consumo ───────────────────────
async function setupConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertQueue(CRITICAL_QUEUE, { durable: true });
  channel.prefetch(1); // Procesar un mensaje a la vez

  console.log(`[actuator_worker] 👂 Esperando mensajes en '${CRITICAL_QUEUE}'…`);

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

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  ACTUATOR WORKER — Ejecutor de Acciones Críticas            ");
  console.log("══════════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupConsumer);
})();
