/**
 * ============================================================
 *  ALERT DETECTOR — Consumidor Kafka Stateful con Ventana Móvil
 * ============================================================
 *  Consume del tópico `sensor_data`, mantiene una ventana de las
 *  últimas 3 lecturas por sensor_id y evalúa dos reglas:
 *
 *   Regla 1 (CRITICAL): vibración > 90  →  alerts_critical
 *   Regla 2 (WARNING) : 3 lecturas consecutivas > 75  →  alerts_warning      (Advertencia)
 *
 *  Publica estado consolidado en `sensor_status` para el dashboard.
 * ============================================================
 */

"use strict";

const { Kafka } = require("kafkajs");

// ── Configuración ──────────────────────────────────────────
// Broker, topicos y tamano de ventana usados para detectar condiciones anormales.
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:29092";
const TOPIC_SENSOR_DATA = "sensor_data";
const TOPIC_SENSOR_STATUS = "sensor_status";
const TOPIC_ALERTS_CRITICAL = "alerts_critical";
const TOPIC_ALERTS_WARNING = "alerts_warning";
const WINDOW_SIZE = 3;

// ── Instancia Kafka ────────────────────────────────────────
const kafka = new Kafka({
  clientId: "alert-detector",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 15 },
});

const consumer = kafka.consumer({ groupId: "alert-detector-group" });
const producer = kafka.producer();

// ── Estado en Memoria — Ventana Móvil por Sensor ───────────
// Historial corto por sensor para validar la regla de 3 lecturas consecutivas.
const sensorWindows = new Map(); // sensor_id → number[]
// Ultimo estado calculado por sensor para mantener continuidad entre lecturas.
const sensorStatuses = new Map(); // sensor_id → "OK" | "WARNING" | "CRITICAL"

// ── Reintento resiliente ───────────────────────────────────
// Reintenta conexiones Kafka para soportar arranques por contenedores.
async function connectWithRetry(label, connectFn, delayMs = 4000) {
  let connected = false;
  while (!connected) {
    try {
      await connectFn();
      connected = true;
      console.log(`[alert_detector] ✅ ${label} conectado.`);
    } catch (err) {
      console.error(`[alert_detector] ⏳ Esperando ${label}… (${err.message})`);
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// Obtiene la letra/codigo del sensor para payloads consumibles por UI y otros servicios.
function toSensorCode(sensorId) {
  const match = String(sensorId).match(/sensor_([A-J])/i);
  return match ? match[1].toUpperCase() : sensorId;
}

// ── Publicar alerta a Kafka ────────────────────────────────
// Publica una alerta en el topico correspondiente cuando se cumple una regla.
async function publishAlert(topic, sensorId, alertPayload) {
  try {
    await producer.send({
      topic,
      messages: [
        {
          key: sensorId,
          value: JSON.stringify(alertPayload),
        },
      ],
    });
    const level = topic === TOPIC_ALERTS_CRITICAL ? "🔴 CRITICAL" : "🟡 WARNING";
    console.log(
      `[alert_detector] ${level} → ${sensorId} | vibración: ${alertPayload.vibration || alertPayload.avg_vibration}`
    );
  } catch (err) {
    console.error("[alert_detector] ❌ Error publicando alerta:", err.message);
  }
}

// ── Publicar estado consolidado (sensor_status) ────────────
// Publica el estado consolidado para que el dashboard siempre tenga la ultima lectura.
async function publishSensorStatus(sensorId, vibration, timestamp, status) {
  const payload = {
    sensor_id: sensorId,
    sensor_code: toSensorCode(sensorId),
    vibration,
    status,
    unit: "mm/s",
    timestamp,
    updated_at: new Date().toISOString(),
  };

  try {
    await producer.send({
      topic: TOPIC_SENSOR_STATUS,
      messages: [{ key: sensorId, value: JSON.stringify(payload) }],
    });
  } catch (err) {
    console.error("[alert_detector] ❌ Error publicando sensor_status:", err.message);
  }
}

// ── Evaluación de Reglas ───────────────────────────────────
// Actualiza la ventana movil y evalua reglas CRITICAL/WARNING para una lectura.
async function evaluateRules(sensorId, vibration, timestamp) {
  if (!sensorWindows.has(sensorId)) {
    sensorWindows.set(sensorId, []);
  }
  const window = sensorWindows.get(sensorId);

  window.push(vibration);
  if (window.length > WINDOW_SIZE) {
    window.shift();
  }

  let status = "OK";

  // ── Regla 1: Alerta Crítica (vibración > 90) ──
  if (vibration > 90) {
    status = "CRITICAL";
    sensorStatuses.set(sensorId, status);
    await publishAlert(TOPIC_ALERTS_CRITICAL, sensorId, {
      alert_type: "CRITICAL",
      sensor_id: sensorId,
      sensor_code: toSensorCode(sensorId),
      vibration: vibration,
      rule: "SINGLE_READING_ABOVE_90",
      message: `Lectura de vibración peligrosa detectada: ${vibration} mm/s`,
      timestamp: timestamp,
      detected_at: new Date().toISOString(),
    });
  }

  // ── Regla 2: Advertencia (3 consecutivas > 75) ──
  if (window.length === WINDOW_SIZE) {
    const allAbove75 = window.every((v) => v > 75);
    if (allAbove75 && status !== "CRITICAL") {
      status = "WARNING";
      sensorStatuses.set(sensorId, status);
      const avg = (window.reduce((a, b) => a + b, 0) / WINDOW_SIZE).toFixed(2);
      await publishAlert(TOPIC_ALERTS_WARNING, sensorId, {
        alert_type: "WARNING",
        sensor_id: sensorId,
        sensor_code: toSensorCode(sensorId),
        readings: [...window],
        avg_vibration: parseFloat(avg),
        rule: "THREE_CONSECUTIVE_ABOVE_75",
        message: `3 lecturas consecutivas elevadas: [${window.join(", ")}] mm/s (prom: ${avg})`,
        timestamp: timestamp,
        detected_at: new Date().toISOString(),
      });
    }
  }

  if (status === "OK" && vibration <= 75) {
    sensorStatuses.set(sensorId, "OK");
  } else if (status === "OK") {
    status = sensorStatuses.get(sensorId) || "OK";
  }

  await publishSensorStatus(sensorId, vibration, timestamp, sensorStatuses.get(sensorId) || status);
}

// ── Main ───────────────────────────────────────────────────
// Punto de entrada: conecta productor/consumidor y evalua cada lectura entrante.
(async () => {
  console.log("═══════════════════════════════════════════════════════");
  console.log("  ALERT DETECTOR — Ventana Móvil Stateful (Kafka)      ");
  console.log("═══════════════════════════════════════════════════════");

  await connectWithRetry("Kafka Producer", () => producer.connect());
  await connectWithRetry("Kafka Consumer", () => consumer.connect());

  await consumer.subscribe({ topic: TOPIC_SENSOR_DATA, fromBeginning: false });

  console.log("[alert_detector] 👂 Escuchando tópico:", TOPIC_SENSOR_DATA);
  console.log("[alert_detector] 📤 Publicando estado en:", TOPIC_SENSOR_STATUS);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        const sensorId = payload.sensor_id;
        const vibration = payload.vibration;
        const timestamp = payload.timestamp;

        await evaluateRules(sensorId, vibration, timestamp);
      } catch (err) {
        console.error("[alert_detector] ❌ Error procesando mensaje:", err.message);
      }
    },
  });
})();
