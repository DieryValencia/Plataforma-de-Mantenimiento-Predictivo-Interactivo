/**
 * ============================================================
 *  OPERATOR CONSOLE BACKEND — Consola Humana de Decisiones
 * ============================================================
 *  Servidor WebSocket (:8082) conectado a RabbitMQ. Declara
 *  una cola exclusiva/temporal ligada al Exchange Fanout
 *  `human_alerts` y despacha cada alerta enriquecida al
 *  frontend del operador.
 * ============================================================
 */

"use strict";

const amqp = require("amqplib");
const { WebSocketServer } = require("ws");

// ── Configuración ──────────────────────────────────────────
// Conexion RabbitMQ, puerto WebSocket y exchange desde donde llegan alertas humanas.
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const WS_PORT = 8082;
const RABBIT_EXCHANGE = "human_alerts";

// ── WebSocket Server ───────────────────────────────────────
// Servidor WebSocket para enviar alertas pendientes al frontend del operador.
const wss = new WebSocketServer({ port: WS_PORT });

wss.on("connection", (ws) => {
  console.log("[operator_console] 🌐 Operador conectado. Total:", wss.clients.size);
  ws.on("close", () => {
    console.log("[operator_console] 🔌 Operador desconectado. Total:", wss.clients.size);
  });
});

// ── Broadcast a todos los operadores ───────────────────────
// Reenvia cada alerta a todos los operadores conectados.
function broadcastToOperators(payload) {
  const message = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) { // WebSocket.OPEN
      client.send(message);
    }
  });
}

// ── Reintento resiliente ───────────────────────────────────
// Reintenta RabbitMQ para que el servicio resista arranques desordenados.
async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      const result = await connectFn();
      connected = true;
      console.log(`[operator_console] ✅ ${label} conectado.`);
      return result;
    } catch (err) {
      console.error(`[operator_console] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// ── Configuración RabbitMQ ─────────────────────────────────
// Crea una cola temporal exclusiva y consume las alertas publicadas por alert_router.
async function setupRabbitConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  // Declarar el Exchange Fanout
  await channel.assertExchange(RABBIT_EXCHANGE, "fanout", { durable: true });

  // Declarar cola exclusiva/temporal (se auto-elimina al desconectarse)
  const q = await channel.assertQueue("", { exclusive: true, autoDelete: true });
  console.log(`[operator_console] 📬 Cola temporal creada: ${q.queue}`);

  // Vincular la cola al Exchange Fanout
  await channel.bindQueue(q.queue, RABBIT_EXCHANGE, "");
  console.log(`[operator_console] 🔗 Cola vinculada al Exchange '${RABBIT_EXCHANGE}'`);

  // Consumir mensajes
  channel.consume(q.queue, (msg) => {
    if (!msg) return;

    try {
      const alertPayload = JSON.parse(msg.content.toString());
      const alertType = alertPayload.type || alertPayload.alert_type;
      const level = alertType === "CRITICAL" ? "🔴" : "🟡";

      console.log(
        `[operator_console] ${level} Alerta recibida → alert_id: ${alertPayload.alert_id} | sensor: ${alertPayload.sensor_id} | tipo: ${alertType}`
      );

      broadcastToOperators({
        type: "HUMAN_ALERT",
        data: {
          ...alertPayload,
          alert_type: alertType,
          type: alertType,
        },
      });

      channel.ack(msg);
    } catch (err) {
      console.error("[operator_console] ❌ Error procesando alerta:", err.message);
      channel.nack(msg, false, false);
    }
  });

  return connection;
}

// ── Main ───────────────────────────────────────────────────
// Punto de entrada: activa WebSocket y suscripcion RabbitMQ para la consola.
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  OPERATOR CONSOLE — Consola de Decisiones Humanas (:8082)   ");
  console.log("══════════════════════════════════════════════════════════════");
  console.log(`[operator_console] 🌐 WebSocket Server en puerto ${WS_PORT}`);

  await connectWithRetry("RabbitMQ", setupRabbitConsumer);
})();
