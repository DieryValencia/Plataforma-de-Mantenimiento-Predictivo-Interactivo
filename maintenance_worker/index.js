/**
 * ============================================================
 *  MAINTENANCE WORKER — Procesador de Órdenes de Mantenimiento
 * ============================================================
 *  Consume de `maintenance_queue` vinculada a:
 *   - Exchange Direct `actions_direct` (mantenimiento inmediato)
 *   - Delayed Exchange (24h y re-escalado tras IGNORAR_10_MINUTOS)
 * ============================================================
 */

"use strict";

const amqp = require("amqplib");

// Datos de conexion y nombres de exchanges/colas desde donde llegan ordenes de mantenimiento.
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const ACTIONS_DIRECT = "actions_direct";
const RK_MAINTENANCE = "maintenance";
const MAINTENANCE_QUEUE = "maintenance_queue";
const DELAYED_EXCHANGE = "delayed_exchange";
const RK_MAINTENANCE_DELAYED = "maintenance_delayed";
const RK_IGNORE_DELAYED = "ignore_delayed";

// Reintenta la conexion a RabbitMQ hasta que el broker este disponible.
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

// Simula el procesamiento de una orden y deja evidencia clara en consola.
function processMaintenanceOrder(payload) {
  const action = payload.chosen_action;
  const isDelayed24h = action === "RECONOCER_Y_ESPERAR_24H";
  const isIgnoreRecheck = action === "IGNORAR_10_MINUTOS";
  const tipo = isIgnoreRecheck
    ? "RE-ESCALADO (Ignorar 10 min expirado)"
    : isDelayed24h
      ? "DIFERIDA (24 horas)"
      : "INMEDIATA";

  console.log("");
  console.log("┌──────────────────────────────────────────────────────────────┐");
  console.log("│  🔧🔧🔧  ORDEN DE MANTENIMIENTO PROCESADA  🔧🔧🔧          │");
  console.log("├──────────────────────────────────────────────────────────────┤");
  console.log(`│  📋 Tipo:        ${tipo}`.padEnd(63) + "│");
  console.log(`│  🆔 Alert ID:    ${payload.alert_id || "N/A"}`.padEnd(63) + "│");
  console.log(`│  🏭 Sensor:      ${payload.sensor_id || "N/A"}`.padEnd(63) + "│");
  console.log(`│  🎯 Acción:      ${action || "N/A"}`.padEnd(63) + "│");
  console.log(`│  ⏰ Despachado:  ${payload.dispatched_at || "N/A"}`.padEnd(63) + "│");
  console.log(`│  🕐 Procesado:   ${new Date().toISOString()}`.padEnd(63) + "│");

  if (isDelayed24h || isIgnoreRecheck) {
    console.log("│                                                             │");
    console.log(
      "│  ⏳ Mensaje liberado por rabbitmq_delayed_message_exchange   │"
    );
    if (isIgnoreRecheck) {
      console.log("│     Re-escalado: el operador ignoró la alerta 10 minutos.  │");
    }
  }

  console.log("│                                                             │");
  console.log("│  ✅ Equipo de mantenimiento notificado                       │");
  console.log(
    "│  📝 Orden de trabajo #MT-" + Date.now().toString().slice(-6) + " creada".padEnd(36) + "│"
  );
  console.log("│  📅 Programada en el calendario de planta                    │");
  console.log("└──────────────────────────────────────────────────────────────┘");
  console.log("");
}

// Declara exchanges/colas, configura prefetch y consume ordenes de mantenimiento.
async function setupConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange(ACTIONS_DIRECT, "direct", { durable: true });
  await channel.assertQueue(MAINTENANCE_QUEUE, { durable: true });
  await channel.bindQueue(MAINTENANCE_QUEUE, ACTIONS_DIRECT, RK_MAINTENANCE);

  await channel.assertExchange(DELAYED_EXCHANGE, "x-delayed-message", {
    durable: true,
    arguments: { "x-delayed-type": "direct" },
  });
  await channel.bindQueue(MAINTENANCE_QUEUE, DELAYED_EXCHANGE, RK_MAINTENANCE_DELAYED);
  await channel.bindQueue(MAINTENANCE_QUEUE, DELAYED_EXCHANGE, RK_IGNORE_DELAYED);

  channel.prefetch(1);

  console.log(
    `[maintenance_worker] 👂 Esperando en '${MAINTENANCE_QUEUE}' ← Direct + Delayed`
  );

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

// Punto de entrada: prepara RabbitMQ y deja el worker escuchando indefinidamente.
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  MAINTENANCE WORKER — Órdenes de Mantenimiento              ");
  console.log("══════════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupConsumer);
})();
