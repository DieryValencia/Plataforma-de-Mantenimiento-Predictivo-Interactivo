/**
 * ============================================================
 *  ACTION DISPATCHER — REST API + Inyección a RabbitMQ
 * ============================================================
 *  Servidor HTTP (:3000) con endpoint POST /decide.
 *  Enruta decisiones humanas vía Exchange Direct `actions_direct`
 *  y Delayed Exchange para tareas diferidas:
 *
 *   - APAGADO_INMEDIATO              → actions_direct / critical
 *   - PROGRAMAR_MANTENIMIENTO_AHORA  → actions_direct / maintenance
 *   - RECONOCER_Y_ESPERAR_24H        → delayed_exchange (24h)
 *   - IGNORAR_10_MINUTOS             → delayed_exchange (10 min)
 * ============================================================
 */

"use strict";

const http = require("http");
const amqp = require("amqplib");

// URL de conexión a RabbitMQ y puerto local del servidor HTTP.
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
// Puerto donde el dashboard envia decisiones humanas con POST /decide.
const HTTP_PORT = 3000;

// Exchange directo para decisiones inmediatas.
const ACTIONS_DIRECT = "actions_direct";
const RK_CRITICAL = "critical";
const RK_MAINTENANCE = "maintenance";

// Colas que consumen las acciones directas.
const CRITICAL_QUEUE = "critical_actions_queue";
const MAINTENANCE_QUEUE = "maintenance_queue";

// Exchange diferido para acciones que esperan un retraso.
const DELAYED_EXCHANGE = "delayed_exchange";
const RK_MAINTENANCE_DELAYED = "maintenance_delayed";
const RK_IGNORE_DELAYED = "ignore_delayed";

// Tiempos de retraso por defecto para las acciones que no se ejecutan de inmediato.
const DELAY_24H_MS = Number(process.env.DELAY_24H_MS) || 86400000;
const DELAY_10MIN_MS = Number(process.env.DELAY_10MIN_MS) || 600000;
// URL del sensor_producer para reflejar en la simulacion la decision elegida.
const SENSOR_CONTROL_URL = process.env.SENSOR_CONTROL_URL || "http://localhost:3002";

// Canal global reutilizado para publicar mensajes en RabbitMQ.
let rabbitChannel = null;

// Intenta conectarse varias veces hasta que RabbitMQ quede disponible.
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

// Configura exchanges y colas en RabbitMQ para el enrutamiento de acciones.
async function setupRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL);
  rabbitChannel = await connection.createChannel();

  await rabbitChannel.assertExchange(ACTIONS_DIRECT, "direct", { durable: true });
  console.log(`[action_dispatcher] 📢 Exchange Direct '${ACTIONS_DIRECT}' declarado.`);

  await rabbitChannel.assertQueue(CRITICAL_QUEUE, { durable: true });
  await rabbitChannel.bindQueue(CRITICAL_QUEUE, ACTIONS_DIRECT, RK_CRITICAL);
  console.log(`[action_dispatcher] 🔗 '${CRITICAL_QUEUE}' ← ${ACTIONS_DIRECT} [${RK_CRITICAL}]`);

  await rabbitChannel.assertQueue(MAINTENANCE_QUEUE, { durable: true });
  await rabbitChannel.bindQueue(MAINTENANCE_QUEUE, ACTIONS_DIRECT, RK_MAINTENANCE);
  console.log(`[action_dispatcher] 🔗 '${MAINTENANCE_QUEUE}' ← ${ACTIONS_DIRECT} [${RK_MAINTENANCE}]`);

  await rabbitChannel.assertExchange(DELAYED_EXCHANGE, "x-delayed-message", {
    durable: true,
    arguments: { "x-delayed-type": "direct" },
  });
  console.log(`[action_dispatcher] ⏰ Delayed Exchange '${DELAYED_EXCHANGE}' declarado.`);

  await rabbitChannel.bindQueue(MAINTENANCE_QUEUE, DELAYED_EXCHANGE, RK_MAINTENANCE_DELAYED);
  await rabbitChannel.bindQueue(MAINTENANCE_QUEUE, DELAYED_EXCHANGE, RK_IGNORE_DELAYED);
  console.log(
    `[action_dispatcher] 🔗 '${MAINTENANCE_QUEUE}' ← ${DELAYED_EXCHANGE} [${RK_MAINTENANCE_DELAYED}, ${RK_IGNORE_DELAYED}]`
  );
  console.log(`[action_dispatcher] ⏱️  Delays: 24h=${DELAY_24H_MS}ms | 10min=${DELAY_10MIN_MS}ms`);
}

// Lee el cuerpo JSON de la petición HTTP entrante.
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

// Establece encabezados CORS básicos para permitir llamadas desde el navegador.
function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

// Envía el control de la acción al servicio de sensor para actualizar el comportamiento.
async function applySensorControl(decision) {
  const body = {
    sensor_id: decision.sensor_id,
    chosen_action: decision.chosen_action,
  };

  if (decision.chosen_action === "IGNORAR_10_MINUTOS") body.duration_ms = DELAY_10MIN_MS;
  if (decision.chosen_action === "RECONOCER_Y_ESPERAR_24H") body.duration_ms = DELAY_24H_MS;

  try {
    const res = await fetch(`${SENSOR_CONTROL_URL}/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await res.json();

    if (data.status !== "ACK") {
      console.warn("[action_dispatcher] ⚠️  Control sensor NACK:", data.error);
    } else {
      console.log(
        `[action_dispatcher] 🎛️  Sensor ${data.sensor_id} → modo ${data.mode}` +
          (data.duration_ms ? ` (${data.duration_ms}ms)` : "")
      );
    }

    return data;
  } catch (err) {
    console.error("[action_dispatcher] ❌ Control sensor falló:", err.message);
    return { status: "NACK", error: err.message };
  }
}

// Enruta la decisión recibida a los exchanges y colas correctos de RabbitMQ.
function routeDecision(decision) {
  const { chosen_action, sensor_id, alert_id, type } = decision;
  const payload = Buffer.from(
    JSON.stringify({
      ...decision,
      alert_type: type || decision.alert_type,
      dispatched_at: new Date().toISOString(),
    })
  );

  const opts = { persistent: true, contentType: "application/json" };

  switch (chosen_action) {
    case "APAGADO_INMEDIATO":
      rabbitChannel.publish(ACTIONS_DIRECT, RK_CRITICAL, payload, opts);
      console.log(
        `[action_dispatcher] 🔴 APAGADO_INMEDIATO → ${ACTIONS_DIRECT}/${RK_CRITICAL} | sensor: ${sensor_id} | alert: ${alert_id}`
      );
      return { status: "ACK", exchange: ACTIONS_DIRECT, routing_key: RK_CRITICAL, action: chosen_action };

    case "PROGRAMAR_MANTENIMIENTO_AHORA":
      rabbitChannel.publish(ACTIONS_DIRECT, RK_MAINTENANCE, payload, opts);
      console.log(
        `[action_dispatcher] 🔧 PROGRAMAR_MANTENIMIENTO → ${ACTIONS_DIRECT}/${RK_MAINTENANCE} | sensor: ${sensor_id}`
      );
      return { status: "ACK", exchange: ACTIONS_DIRECT, routing_key: RK_MAINTENANCE, action: chosen_action };

    case "RECONOCER_Y_ESPERAR_24H":
      rabbitChannel.publish(DELAYED_EXCHANGE, RK_MAINTENANCE_DELAYED, payload, {
        ...opts,
        headers: { "x-delay": DELAY_24H_MS },
      });
      console.log(
        `[action_dispatcher] ⏰ ESPERAR_24H → ${DELAYED_EXCHANGE} (${DELAY_24H_MS}ms) | sensor: ${sensor_id}`
      );
      return {
        status: "ACK",
        exchange: DELAYED_EXCHANGE,
        routing_key: RK_MAINTENANCE_DELAYED,
        action: chosen_action,
        delay_ms: DELAY_24H_MS,
      };

    case "IGNORAR_10_MINUTOS":
      rabbitChannel.publish(DELAYED_EXCHANGE, RK_IGNORE_DELAYED, payload, {
        ...opts,
        headers: { "x-delay": DELAY_10MIN_MS },
      });
      console.log(
        `[action_dispatcher] ⏭️  IGNORAR_10_MIN → ${DELAYED_EXCHANGE} (${DELAY_10MIN_MS}ms) | sensor: ${sensor_id}`
      );
      return {
        status: "ACK",
        exchange: DELAYED_EXCHANGE,
        routing_key: RK_IGNORE_DELAYED,
        action: chosen_action,
        delay_ms: DELAY_10MIN_MS,
      };

    default:
      console.warn(`[action_dispatcher] ⚠️  Acción desconocida: ${chosen_action}`);
      return { status: "NACK", error: `Acción desconocida: ${chosen_action}` };
  }
}

// Servidor HTTP que atiende POST /decide, GET /health y permisos CORS.
const server = http.createServer(async (req, res) => {
  setCors(res);

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === "POST" && req.url === "/decide") {
    try {
      const body = await parseBody(req);

      if (!body.chosen_action || !body.sensor_id) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Campos requeridos: chosen_action, sensor_id" }));
        return;
      }

      // Primero se publica la decision en RabbitMQ; si fue valida, tambien se aplica al simulador.
      const result = routeDecision(body);
      if (result.status === "ACK") {
        const sensorResult = await applySensorControl(body);
        if (sensorResult.mode) result.sensor_mode = sensorResult.mode;
        if (sensorResult.duration_ms) result.sensor_duration_ms = sensorResult.duration_ms;
      }
      const statusCode = result.status === "ACK" ? 200 : 400;

      res.writeHead(statusCode, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "healthy", timestamp: new Date().toISOString() }));
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// Inicio de la aplicación: conectamos RabbitMQ y arrancamos el servidor HTTP.
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
