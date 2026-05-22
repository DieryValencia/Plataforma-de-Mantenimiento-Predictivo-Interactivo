/**
 * ============================================================
 *  NOTIFICATION SERVICE — Alertas al celular + decisiones móviles
 * ============================================================
 *  - Cola de envío a Telegram (anti-saturación)
 *  - Un solo getUpdates en vuelo (evita Conflict de Telegram)
 *  - Fanout human_alerts → Telegram / ntfy / panel /m
 * ============================================================
 */

"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const amqp = require("amqplib");

// Configuracion de integraciones: RabbitMQ, API local de decision y URL publica del panel movil.
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://admin:admin123@localhost:5672";
const RABBIT_EXCHANGE = "human_alerts";
const HTTP_PORT = Number(process.env.HTTP_PORT) || 3003;
const DECIDE_URL = process.env.DECIDE_URL || "http://localhost:3001/decide";
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "http://localhost:3003").replace(/\/$/, "");

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

// Banderas para controlar si se notifican solo criticas o tambien advertencias.
const NOTIFY_CRITICAL_ONLY = process.env.NOTIFY_CRITICAL_ONLY !== "false";
const NOTIFY_WARNINGS = process.env.NOTIFY_WARNINGS === "true";

const NTFY_TOPIC = process.env.NTFY_TOPIC || "";
const NTFY_SERVER = (process.env.NTFY_SERVER || "https://ntfy.sh").replace(/\/$/, "");

// Limites de ritmo para no saturar Telegram ni duplicar notificaciones.
const TELEGRAM_QUEUE_DELAY_MS = Number(process.env.TELEGRAM_QUEUE_DELAY_MS) || 5000;
const TELEGRAM_MIN_INTERVAL_MS = Number(process.env.TELEGRAM_MIN_INTERVAL_MS) || 3500;
const MAX_NOTIFY_QUEUE = Number(process.env.MAX_NOTIFY_QUEUE) || 25;

const ACTION_LABELS = {
  APAGADO_INMEDIATO: "⚡ Apagar",
  IGNORAR_10_MINUTOS: "⏭ Ignorar 10m",
  PROGRAMAR_MANTENIMIENTO_AHORA: "🔧 Mantenimiento",
  RECONOCER_Y_ESPERAR_24H: "⏳ Esperar 24h",
};

const ACTION_CODES = {
  APAGADO_INMEDIATO: "OFF",
  IGNORAR_10_MINUTOS: "IGN",
  PROGRAMAR_MANTENIMIENTO_AHORA: "PGM",
  RECONOCER_Y_ESPERAR_24H: "R24",
};

// Traduccion inversa del codigo corto de Telegram a la accion interna.
const CODE_TO_ACTION = Object.fromEntries(
  Object.entries(ACTION_CODES).map(([k, v]) => [v, k])
);

// Estado en memoria: alertas pendientes, callbacks de Telegram y cola de envio.
const alertsStore = new Map();
const callbackStore = new Map();
const notifyQueue = [];
const queuedAlertIds = new Set();

let telegramOffset = 0;
let notifyDraining = false;
let lastTelegramSentAt = 0;
let telegramPollRunning = false;
const MAX_ALERTS_STORE = 40;

// Pausa asincrona usada para espaciar polls y envios.
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Genera una llave corta para identificar callbacks de Telegram sin exponer todo el payload.
function shortKey() {
  return crypto.randomBytes(4).toString("hex");
}

// Decide si una alerta amerita push segun severidad y configuracion.
function shouldNotify(alert) {
  if (alert.type === "CRITICAL") return true;
  if (NOTIFY_WARNINGS && alert.type === "WARNING") return true;
  if (NOTIFY_CRITICAL_ONLY) return false;
  return true;
}

// Telegram rechaza botones URL con localhost/127.0.0.1; solo se agrega el enlace si es publico.
function hasPublicMobileUrl() {
  try {
    const url = new URL(PUBLIC_BASE_URL);
    return !["localhost", "127.0.0.1", "0.0.0.0"].includes(url.hostname);
  } catch (_) {
    return false;
  }
}

// Reintenta servicios externos hasta que esten disponibles.
async function connectWithRetry(label, fn, delayMs = 4000) {
  while (true) {
    try {
      return await fn();
    } catch (err) {
      console.error(`[notification] ⏳ ${label}: ${err.message}`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// Guarda alertas recientes y recorta el historial para no crecer indefinidamente.
function storeAlert(alert) {
  alertsStore.set(alert.alert_id, { ...alert, received_at: new Date().toISOString() });
  while (alertsStore.size > MAX_ALERTS_STORE) {
    const first = alertsStore.keys().next().value;
    alertsStore.delete(first);
  }
}

// ── Cola de notificaciones ─────────────────────────────────
function enqueueNotification(alert) {
  if (queuedAlertIds.has(alert.alert_id)) {
    console.log(`[notification] ⏭️  Duplicado en cola: ${alert.alert_id}`);
    return;
  }
  if (notifyQueue.length >= MAX_NOTIFY_QUEUE) {
    const dropped = notifyQueue.shift();
    if (dropped) queuedAlertIds.delete(dropped.alert_id);
    console.log(`[notification] ⚠️  Cola llena — descartada alerta antigua`);
  }
  notifyQueue.push(alert);
  queuedAlertIds.add(alert.alert_id);
  console.log(
    `[notification] 📥 Encolada ${alert.type} sensor ${alert.sensor_id} (cola: ${notifyQueue.length})`
  );
  startNotifyDrain();
}

// Activa el drenado de cola solo si no hay otro drenado corriendo.
function startNotifyDrain() {
  if (!notifyDraining) drainNotificationQueue();
}

// Procesa la cola respetando intervalos minimos entre envios a Telegram.
async function drainNotificationQueue() {
  if (notifyDraining) return;
  notifyDraining = true;

  while (notifyQueue.length > 0) {
    const alert = notifyQueue.shift();

    try {
      const elapsed = Date.now() - lastTelegramSentAt;
      if (elapsed < TELEGRAM_MIN_INTERVAL_MS) {
        await sleep(TELEGRAM_MIN_INTERVAL_MS - elapsed);
      }

      if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
        await sendTelegramAlert(alert);
        lastTelegramSentAt = Date.now();
      }

      await sendNtfyPush(alert);
      console.log(`[notification] ✅ Enviada ${alert.type} → ${alert.sensor_id}`);
    } catch (err) {
      console.error(`[notification] ❌ Envío fallido: ${err.message}`);
    } finally {
      queuedAlertIds.delete(alert.alert_id);
    }

    if (notifyQueue.length > 0) {
      await sleep(TELEGRAM_QUEUE_DELAY_MS);
    }
  }

  notifyDraining = false;
}

// ── Telegram API ───────────────────────────────────────────
async function telegramApi(method, body = {}) {
  if (!TELEGRAM_BOT_TOKEN) return null;
  let url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;
  let options = {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };

  if (method === "getUpdates") {
    const q = new URLSearchParams({
      offset: String(body.offset ?? telegramOffset),
      timeout: "25",
      allowed_updates: JSON.stringify(["callback_query"]),
    });
    url += `?${q}`;
    options = { method: "GET" };
  }

  const res = await fetch(url, options);
  const data = await res.json();
  if (!data.ok) {
    throw new Error(data.description || "Telegram API error");
  }
  return data.result;
}

// Construye botones inline de Telegram y asocia cada boton con su contexto de alerta.
function buildTelegramKeyboard(alert) {
  const rows = [];
  const options = alert.options || [];
  for (let i = 0; i < options.length; i += 2) {
    const row = [];
    for (let j = i; j < Math.min(i + 2, options.length); j++) {
      const action = options[j];
      const code = ACTION_CODES[action];
      const key = shortKey();
      callbackStore.set(key, {
        alert_id: alert.alert_id,
        sensor_id: alert.sensor_id,
        type: alert.type,
        chosen_action: action,
      });
      row.push({
        text: ACTION_LABELS[action] || action.replace(/_/g, " "),
        callback_data: `d:${key}:${code}`,
      });
    }
    rows.push(row);
  }
  if (hasPublicMobileUrl()) {
    rows.push([
      {
        text: "📱 Abrir panel móvil",
        url: `${PUBLIC_BASE_URL}/m?alert=${alert.alert_id}`,
      },
    ]);
  }
  return { inline_keyboard: rows };
}

// Envia a Telegram el mensaje principal de alerta con botones de decision.
async function sendTelegramAlert(alert) {
  const icon = alert.type === "CRITICAL" ? "🔴" : "🟡";
  const text = [
    `${icon} *${alert.type} — ACCIÓN REQUERIDA*`,
    ``,
    `🏭 Sensor: \`${alert.sensor_id}\``,
    alert.vibration != null ? `📊 Vibración: *${alert.vibration}* mm/s` : "",
    alert.message ? `📝 ${alert.message}` : "",
    `🆔 \`${alert.alert_id}\``,
    ``,
    `_Toca un botón para ejecutar la decisión:_`,
  ]
    .filter(Boolean)
    .join("\n");

  await telegramApi("sendMessage", {
    chat_id: TELEGRAM_CHAT_ID,
    text,
    parse_mode: "Markdown",
    reply_markup: buildTelegramKeyboard(alert),
  });

  console.log(`[notification] 📲 Telegram → ${alert.type} sensor ${alert.sensor_id}`);
}

// Confirma al usuario de Telegram que su toque fue recibido.
async function answerTelegramCallback(callbackQueryId, text) {
  await telegramApi("answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
    show_alert: text.length > 80,
  });
}

// Edita el mensaje original para dejar constancia de que la decision ya se ejecuto.
async function editTelegramMessage(chatId, messageId, text) {
  try {
    await telegramApi("editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text,
      parse_mode: "Markdown",
    });
  } catch (_) {}
}

// Envia una notificacion push via ntfy si el topico esta configurado.
async function sendNtfyPush(alert) {
  if (!NTFY_TOPIC) return;

  const title = `${alert.type} — Sensor ${alert.sensor_id}`;
  const body = alert.message || "Decisión requerida en planta IoT";
  const url = `${PUBLIC_BASE_URL}/m?alert=${encodeURIComponent(alert.alert_id)}`;

  await fetch(`${NTFY_SERVER}/${NTFY_TOPIC}`, {
    method: "POST",
    headers: {
      Title: title,
      Priority: alert.type === "CRITICAL" ? "urgent" : "high",
      Tags: alert.type === "CRITICAL" ? "rotating_light" : "warning",
      Click: url,
    },
    body: `${body}\n\nAbrir: ${url}`,
  });
}

// Llama al action_dispatcher para ejecutar la decision elegida desde movil/Telegram.
async function executeDecision(decision) {
  const res = await fetch(DECIDE_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      alert_id: decision.alert_id,
      sensor_id: decision.sensor_id,
      type: decision.type,
      alert_type: decision.type,
      chosen_action: decision.chosen_action,
    }),
  });
  const data = await res.json();
  if (!res.ok || data.status !== "ACK") {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  alertsStore.delete(decision.alert_id);
  queuedAlertIds.delete(decision.alert_id);
  return data;
}

/**
 * Un único bucle de long-polling. NO usar setInterval: solapa getUpdates
 * y provoca "Conflict: terminated by other getUpdates request".
 */
async function pollTelegramUpdatesLoop() {
  if (!TELEGRAM_BOT_TOKEN || telegramPollRunning) return;
  telegramPollRunning = true;

  console.log("[notification] 🔄 Telegram polling iniciado (un solo hilo)");

  while (telegramPollRunning) {
    try {
      const updates = await telegramApi("getUpdates", { offset: telegramOffset });
      if (updates?.length) {
        for (const update of updates) {
          telegramOffset = update.update_id + 1;
          const cq = update.callback_query;
          if (!cq?.data?.startsWith("d:")) continue;

          const [, key, code] = cq.data.split(":");
          const ctx = callbackStore.get(key);
          const action = CODE_TO_ACTION[code];

          if (!ctx || !action) {
            await answerTelegramCallback(cq.id, "❌ Sesión expirada");
            continue;
          }

          try {
            const result = await executeDecision({ ...ctx, chosen_action: action });
            callbackStore.delete(key);
            const label = ACTION_LABELS[action] || action;
            await answerTelegramCallback(cq.id, `✅ ${label} aplicado`);
            await editTelegramMessage(
              cq.message.chat.id,
              cq.message.message_id,
              `✅ *Decisión ejecutada*\n\n🏭 Sensor \`${ctx.sensor_id}\`\n🎯 ${label}\n🎛 \`${result.sensor_mode || "OK"}\``
            );
          } catch (err) {
            await answerTelegramCallback(cq.id, `❌ ${err.message}`);
          }
        }
      }
    } catch (err) {
      const msg = err.message || "";
      if (msg.includes("Conflict") || msg.includes("terminated by other getUpdates")) {
        console.error(
          "[notification] ⚠️  Telegram Conflict: hay otro getUpdates activo (¿varios contenedores o polling duplicado?). Reintento en 5s…"
        );
        await sleep(5000);
      } else {
        console.error("[notification] ⚠️  Telegram poll:", msg);
        await sleep(2000);
      }
    }
  }
}

// Consume alertas humanas desde RabbitMQ y las guarda/encola para notificacion.
async function startRabbitConsumer() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await channel.assertExchange(RABBIT_EXCHANGE, "fanout", { durable: true });

  const q = await channel.assertQueue("notification_human_alerts", {
    exclusive: false,
    durable: true,
    autoDelete: false,
  });
  await channel.bindQueue(q.queue, RABBIT_EXCHANGE, "");

  console.log(`[notification] 📬 Cola '${q.queue}' ← fanout '${RABBIT_EXCHANGE}'`);
  console.log(
    `[notification] ⏱️  Cola Telegram: delay ${TELEGRAM_QUEUE_DELAY_MS}ms | mín ${TELEGRAM_MIN_INTERVAL_MS}ms entre envíos`
  );

  channel.consume(q.queue, (msg) => {
    if (!msg) return;
    try {
      const alert = JSON.parse(msg.content.toString());
      storeAlert(alert);

      if (shouldNotify(alert)) {
        enqueueNotification(alert);
      } else {
        console.log(`[notification] ℹ️  ${alert.type} almacenada (sin push móvil)`);
      }
      channel.ack(msg);
    } catch (err) {
      console.error("[notification] ❌ Alerta:", err.message);
      channel.nack(msg, false, false);
    }
  });
}

// Lee el HTML movil servido desde public/.
function readStatic(file) {
  return fs.readFileSync(path.join(__dirname, "public", file), "utf8");
}

// Lee y parsea JSON de peticiones HTTP entrantes.
function parseBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(new Error("JSON inválido"));
      }
    });
    req.on("error", reject);
  });
}

// API HTTP del servicio: salud, alertas pendientes, decision movil y pagina /m.
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, PUBLIC_BASE_URL);

  if (req.method === "GET" && url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "healthy",
        telegram: Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID),
        telegram_poll_single: telegramPollRunning,
        notify_queue_length: notifyQueue.length,
        ntfy: Boolean(NTFY_TOPIC),
        pending_alerts: alertsStore.size,
      })
    );
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/alerts/pending") {
    const list = [...alertsStore.values()]
      .sort((a, b) => (b.received_at || "").localeCompare(a.received_at || ""))
      .slice(0, 20);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ alerts: list }));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/decide") {
    try {
      const body = await parseBody(req);
      const result = await executeDecision({
        alert_id: body.alert_id,
        sensor_id: body.sensor_id,
        type: body.type || "CRITICAL",
        chosen_action: body.chosen_action,
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  if (req.method === "GET" && (url.pathname === "/m" || url.pathname === "/mobile")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(readStatic("mobile.html"));
    return;
  }

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// Punto de entrada: configura Telegram opcional, RabbitMQ y servidor HTTP.
(async () => {
  console.log("══════════════════════════════════════════════════════════════");
  console.log("  NOTIFICATION SERVICE — Cola + Telegram (un solo poll)        ");
  console.log("══════════════════════════════════════════════════════════════");

  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
    try {
      await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/deleteWebhook`);
    } catch (_) {}
    pollTelegramUpdatesLoop();
  } else {
    console.log("[notification] ⚠️  Telegram deshabilitado");
  }

  await connectWithRetry("RabbitMQ", startRabbitConsumer);

  server.listen(HTTP_PORT, () => {
    console.log(`[notification] 🌐 HTTP :${HTTP_PORT}`);
  });
})();
