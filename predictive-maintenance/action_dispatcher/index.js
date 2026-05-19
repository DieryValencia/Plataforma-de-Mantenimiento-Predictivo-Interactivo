/**
 * ============================================================
 *  ACTION DISPATCHER — REST API + Inyección a RabbitMQ
 * ============================================================
 *  Servidor HTTP (:3000) con endpoint POST /decide.
 *  Recibe decisiones humanas del frontend y las enruta a las
 *  colas apropiadas de RabbitMQ:
 *
 *   - APAGADO_INMEDIATO         → critical_actions_queue (directo)
 *   - PROGRAMAR_MANTENIMIENTO   → maintenance_queue (directo)
 *   - RECONOCER_Y_ESPERAR_24H   → delayed_exchange (15s delay)
 *   - IGNORAR_10_MINUTOS        → log y descarte
 * ============================================================
 */

"use strict";

const http = require("http");
const amqp = require("amqplib");

// ── Configuración ──────────────────────────────────────────
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const HTTP_PORT = 3000;
const CRITICAL_QUEUE = "critical_actions_queue";
const MAINTENANCE_QUEUE = "maintenance_queue";
const DELAYED_EXCHANGE = "delayed_exchange";

// ── Estado RabbitMQ ────────────────────────────────────────
let rabbitChannel = null;

// ── Reintento resiliente ───────────────────────────────────
async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      await connectFn();
      connected = true;
      console.log(`[action_dispatcher] ✅ ${label} conectado.`);
    } catch (err) {
      console.error(`[action_dispatcher] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// ── Configuración RabbitMQ ─────────────────────────────────
async function setupRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await connection.createChannel();

  // Declarar colas durables
  await rabbitChannel.assertQueue(CRITICAL_QUEUE, { durable: true });
  console.log(`[action_dispatcher] 📦 Cola '${CRITICAL_QUEUE}' declarada.`);

  await rabbitChannel.assertQueue(MAINTENANCE_QUEUE, { durable: true });
  console.log(`[action_dispatcher] 📦 Cola '${MAINTENANCE_QUEUE}' declarada.`);

  // Declarar Delayed Exchange (plugin rabbitmq_delayed_message_exchange)
  await rabbitChannel.assertExchange(DELAYED_EXCHANGE, "x-delayed-message", {
    durable: true,
    arguments: { "x-delayed-type": "direct" },
  });
  console.log(`[action_dispatcher] ⏰ Delayed Exchange '${DELAYED_EXCHANGE}' declarado.`);

  // Vincular la cola de mantenimiento al delayed exchange
  await rabbitChannel.bindQueue(MAINTENANCE_QUEUE, DELAYED_EXCHANGE, "maintenance_delayed");
  console.log(`[action_dispatcher] 🔗 '${MAINTENANCE_QUEUE}' vinculada a '${DELAYED_EXCHANGE}'`);
}

// ── Parsear cuerpo HTTP ────────────────────────────────────
function parseBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => {
      try {
        resolve(JSON.parse(data));
      } catch (err) {
        reject(new Error("JSON inválido"));
      }
    });
    req.on("error", reject);
  });
}

// ── Headers CORS ───────────────────────────────────────────
function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

// ── Ruteo de Decisión ──────────────────────────────────────
function routeDecision(decision) {
  const { chosen_action, sensor_id, alert_type } = decision;
  const payload = Buffer.from(JSON.stringify({
    ...decision,
    dispatched_at: new Date().toISOString(),
  }));

  switch (chosen_action) {
    case "APAGADO_INMEDIATO":
      rabbitChannel.sendToQueue(CRITICAL_QUEUE, payload, { persistent: true });
      console.log(`[action_dispatcher] 🔴 APAGADO_INMEDIATO → ${CRITICAL_QUEUE} | sensor: ${sensor_id}`);
      return { status: "ACK", queue: CRITICAL_QUEUE, action: chosen_action };

    case "PROGRAMAR_MANTENIMIENTO_AHORA":
      rabbitChannel.sendToQueue(MAINTENANCE_QUEUE, payload, { persistent: true });
      console.log(`[action_dispatcher] 🔧 PROGRAMAR_MANTENIMIENTO → ${MAINTENANCE_QUEUE} | sensor: ${sensor_id}`);
      return { status: "ACK", queue: MAINTENANCE_QUEUE, action: chosen_action };

    case "RECONOCER_Y_ESPERAR_24H":
      rabbitChannel.publish(DELAYED_EXCHANGE, "maintenance_delayed", payload, {
        persistent: true,
        headers: { "x-delay": 15000 }, // 15 seg (simulación de 24h)
      });
      console.log(`[action_dispatcher] ⏰ ESPERAR_24H → ${DELAYED_EXCHANGE} (15s delay) | sensor: ${sensor_id}`);
      return { status: "ACK", queue: DELAYED_EXCHANGE, action: chosen_action, delay: "15s (simulated 24h)" };

    case "IGNORAR_10_MINUTOS":
      console.log(`[action_dispatcher] ⏭️  IGNORAR_10_MIN → descartado | sensor: ${sensor_id}`);
      return { status: "ACK", action: chosen_action, note: "Alerta ignorada por 10 minutos" };

    default:
      console.warn(`[action_dispatcher] ⚠️  Acción desconocida: ${chosen_action}`);
      return { status: "NACK", error: `Acción desconocida: ${chosen_action}` };
  }
}

// ── Servidor HTTP ──────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  setCors(res);

  // Preflight CORS
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  // POST /decide
  if (req.method === "POST" && req.url === "/decide") {
    try {
      const body = await parseBody(req);

      if (!body.chosen_action || !body.sensor_id) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Campos requeridos: chosen_action, sensor_id" }));
        return;
      }

      const result = routeDecision(body);
      const statusCode = result.status === "ACK" ? 200 : 400;

      res.writeHead(statusCode, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Healthcheck
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "healthy", timestamp: new Date().toISOString() }));
    return;
  }

  // 404
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// ── Main ───────────────────────────────────────────────────
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  ACTION DISPATCHER — REST API :3000 + RabbitMQ Router       ");
  console.log("══════════════════════════════════════════════════════════════");

  await connectWithRetry("RabbitMQ", setupRabbitMQ);

  server.listen(HTTP_PORT, () => {
    console.log(`[action_dispatcher] 🌐 HTTP Server escuchando en puerto ${HTTP_PORT}`);
    console.log(`[action_dispatcher] 📡 POST /decide → Despacho de decisiones`);
  });
})();
