# 🏭 Plataforma Híbrida de Mantenimiento Predictivo IoT (Kafka & RabbitMQ)

Este proyecto implementa una solución de nivel industrial para el **Mantenimiento Predictivo IoT** utilizando una arquitectura híbrida de mensajería asíncrona distribuyendo el trabajo en dos autopistas clave:
1. **Flujo de Ingesta Masiva y Tiempo Real (Apache Kafka):** Ingesta continua de sensores con afinidad por partición y procesamiento de ventanas móviles para detección de eventos.
2. **Flujo Transaccional y Toma de Decisiones (RabbitMQ):** Distribución selectiva de alertas enriquecidas para operadores, comandos hacia actuadores y planificación en diferido (Delayed Message Exchange).

---

## 🏗️ Arquitectura General del Sistema

```mermaid
graph TD
    subgraph Capa de Ingesta y Análisis (Kafka KRaft)
        SP[sensor_producer] -->|sensor_data / key: sensor_id| K_SD[(Tópico: sensor_data)]
        K_SD --> AD[alert_detector]
        AD -->|alerts_critical| K_AC[(Tópico: alerts_critical)]
        AD -->|alerts_warning| K_AW[(Tópico: alerts_warning)]
        
        AD -->|sensor_status| K_SS[(Tópico: sensor_status)]
        K_SS --> PM[plant_monitor_backend :8081]
        K_AC --> PM
        K_AW --> PM
    end

    subgraph Capa Transaccional y Humana (RabbitMQ)
        K_AC --> AR[alert_router]
        K_AW --> AR
        AR -->|Interoperabilidad| RX_HA{Exchange Fanout: human_alerts}
        
        RX_HA --> OC[operator_console_backend :8082]
        RX_HA --> NS[notification_service :3003]
        
        NS -->|Telegram / ntfy| PHONE[📱 Operador móvil]
        NS -->|POST /decide| DISP[action_dispatcher :3001]
        DASH[dashboard.html] -->|fetch POST /decide| DISP
        PHONE -->|Web /m| NS
        
        DISP -->|actions_direct [critical]| AW[actuator_worker]
        DISP -->|actions_direct [maintenance]| MW[maintenance_worker]
        DISP -->|delayed_exchange 24h / 10min| MW
    end

    subgraph Interfaz de Usuario
        PM -.->|WebSocket :8081| DASH
        OC -.->|WebSocket :8082| DASH
    end
```

---

## 📁 Estructura del Proyecto

El sistema está dividido en microservicios modulares e independientes utilizando Node.js:

```text
predictive-maintenance/
├── docker-compose.yml          # Orquestación de infraestructura y microservicios
├── README.md                   # Documentación técnica general
├── rabbitmq/
│   └── Dockerfile              # Dockerfile de RabbitMQ con instalación offline del plugin x-delay
├── sensor_producer/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # Productor de simulación de 10 sensores (Kafka)
├── alert_detector/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # Consumidor de Kafka con Ventana Móvil de 3 lecturas
├── alert_router/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # Puente de Interoperabilidad Kafka -> RabbitMQ
├── plant_monitor_backend/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # WebSocket Server (Telemetría en tiempo real)
├── operator_console_backend/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # WebSocket Server (Despacho de decisiones a operador)
├── action_dispatcher/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # REST API y enrutador de decisiones
├── actuator_worker/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # Worker simulador de actuador industrial
├── maintenance_worker/
│   ├── package.json
│   ├── Dockerfile
│   └── index.js                # Worker para tareas de mantenimiento inmediato/diferido
├── notification_service/
│   ├── package.json
│   ├── Dockerfile
│   ├── .env.example            # Telegram / ntfy (copiar a .env)
│   ├── index.js                # Push móvil + API decisiones
│   └── public/mobile.html      # Panel táctil para celular
└── dashboard/
    └── dashboard.html          # Interfaz web de operaciones con Tailwind CSS
```

---

## 🔌 Asignación de Puertos y Servicios

| Puerto | Servicio | Protocolo / Uso |
| :--- | :--- | :--- |
| **9092** | `kafka` | Tráfico externo de Kafka |
| **29092** | `kafka` | Tráfico interno de Docker Compose |
| **5672** | `rabbitmq` | AMQP Mensajería |
| **15672** | `rabbitmq` | Consola de Administración (Admin: `admin` / Pass: `admin123`) |
| **8081** | `plant_monitor_backend` | WebSocket Server - Envío de Telemetría |
| **8082** | `operator_console_backend`| WebSocket Server - Despacho de Decisiones al Operador |
| **3001** | `action_dispatcher` | HTTP POST - Envío de Decisiones (Remapeado para evitar conflictos con Grafana local) |
| **3002** | `sensor_producer` | HTTP POST /control - Impacto de decisiones en simulación |
| **3003** | `notification_service` | Telegram / ntfy + panel móvil `/m` |

---

## 📱 Notificaciones al celular del operador

El microservicio `notification_service` consume el mismo Fanout `human_alerts` y envía alertas **críticas** al móvil con botones para decidir.

### Opción A — Telegram (recomendada)

1. En Telegram, abre [@BotFather](https://t.me/BotFather) → `/newbot` → copia el **token**.
2. Envía `/start` a tu bot y obtén el **chat_id**:
   ```bash
   curl "https://api.telegram.org/bot<TU_TOKEN>/getUpdates"
   ```
   Busca `"chat":{"id":123456789}`.
3. Copia la plantilla de variables:
   ```bash
   cp notification_service/.env.example notification_service/.env
   ```
4. Edita `notification_service/.env`:
   ```env
   TELEGRAM_BOT_TOKEN=123456:ABC...
   TELEGRAM_CHAT_ID=123456789
   ```
5. En `docker-compose.yml`, en el servicio `notification_service`, referencia el archivo (o pega las variables en `environment`):
   ```yaml
   env_file:
     - ./notification_service/.env
   ```
6. Reinicia: `docker compose up --build -d notification_service`

Recibirás un mensaje con botones (**Apagar**, **Ignorar 10m**, etc.). Al pulsar, se ejecuta `POST /decide` y el sensor en la simulación cambia de estado.

### Opción B — Panel web en el celular

Abre en el navegador del teléfono (misma red Wi‑Fi):

**http://localhost:3003/m**

Lista alertas pendientes con botones grandes. También llega como enlace desde Telegram o ntfy.

### Opción C — ntfy (push simple)

1. Instala la app [ntfy](https://ntfy.sh) en el celular.
2. Suscríbete a un topic (ej. `planta-iot-operador-david`).
3. En `.env`: `NTFY_TOPIC=planta-iot-operador-david`
4. La notificación abre el panel `/m` al tocar.

### Verificar

```bash
docker logs -f notification_service
curl http://localhost:3003/health
```

### Cola de notificaciones y error `getUpdates Conflict`

Las alertas críticas se **encolan** antes de enviarse a Telegram (delay configurable, deduplicación por `alert_id`).

El error `Conflict: terminated by other getUpdates request` **no es por cantidad de mensajes**, sino porque **dos clientes llaman `getUpdates` a la vez** con el mismo bot. Causas habituales:

1. **Bug corregido:** antes se usaba `setInterval` cada 1 s mientras el long-poll dura ~25 s → varias peticiones solapadas.
2. **Dos instancias** del servicio (p. ej. `docker compose` + `node index.js` local con el mismo token).
3. **Webhook activo** además de polling (se llama `deleteWebhook` al arrancar).

Solución: un solo contenedor `notification_service`, un solo token, reiniciar tras el cambio:

```bash
docker compose up --build -d notification_service
```

### Simulación: una crítica cada 8 horas

En `sensor_producer`, la telemetría ya no es aleatoria pura: como máximo **una lectura >90 mm/s cada 8 h** en toda la planta (`CRITICAL_INTERVAL_MS=28800000`). Para pruebas rápidas:

```yaml
CRITICAL_INTERVAL_MS: "120000"   # 2 minutos en sensor_producer
```

---

## 🚀 Despliegue del Sistema

### Requisitos Previos
* **Docker Desktop** instalado y en ejecución.

### Instrucciones de Ejecución

1. **Iniciar el clúster de microservicios**:
   ```bash
   cd predictive-maintenance
   docker compose up --build -d
   ```

2. **Verificar el estado de los contenedores**:
   ```bash
   docker compose ps
   ```
   Deberías ver los 11 contenedores en estado `Up` y `healthy`.

3. **Ejecutar la Interfaz Web**:
   Abre el archivo `dashboard/dashboard.html` directamente en tu navegador preferido. Puedes abrirlo con doble clic en tu explorador de archivos o con el siguiente comando en Windows PowerShell/Command Prompt:
   ```bash
   start dashboard/dashboard.html
   ```

> [!NOTE]
> **¿Mensaje "Upgrade Required" en el navegador?**
> Si intentas acceder directamente a `http://localhost:8081` o `http://localhost:8082` usando la barra de direcciones del navegador, verás un error `Upgrade Required`. Esto es **completamente normal**. Esos puertos exponen servidores **WebSocket puros** y requieren una cabecera de conexión especial iniciada desde JavaScript. Abre siempre el archivo `dashboard.html` para interactuar con la plataforma de forma visual.

---

## 🕵️ Comandos de Inspección y Monitoreo (CLI)

Para validar que la arquitectura de mensajería híbrida funciona correctamente, ejecuta los siguientes comandos en tu consola:

### 1. Ingesta de Telemetría (Kafka)
Inspecciona si el simulador está transmitiendo lecturas al tópico de Kafka:
```bash
docker logs -f sensor_producer
```

### 2. Detección de Alertas en Ventana Móvil
Verifica cómo el consumidor detecta las anomalías basadas en estado en tiempo real (Lecturas > 90 o 3 consecutivas > 75):
```bash
docker logs -f alert_detector
```

### 3. Enrutamiento e Interoperabilidad Kafka -> RabbitMQ
Verifica el procesamiento del puente de mensajería enriqueciendo los payloads con opciones antes de inyectarlos a RabbitMQ:
```bash
docker logs -f alert_router
```

### 4. Ejecución Inmediata de Apagado Industrial (Actuador)
Toma la decisión de **"APAGADO INMEDIATO"** en una alerta crítica desde el dashboard y verifica el log en el worker del actuador:
```bash
docker logs -f actuator_worker
```

### 5. Ejecución Diferida mediante RabbitMQ (Mantenimiento)
Toma la decisión de **"RECONOCER Y ESPERAR 24H"** (retraso real de 24 h, `86400000` ms) o **"IGNORAR 10 MINUTOS"** (retraso de 10 min, `600000` ms) y verifica en el worker de mantenimiento cuando el mensaje se libera del Delayed Exchange:
```bash
docker logs -f maintenance_worker
```

> Para pruebas locales más rápidas (barra CD / reactivación), sobreescribe en `docker-compose.yml`
> `DELAY_10MIN_MS` y `DELAY_24H_MS` en **sensor_producer** y **action_dispatcher** (ej. `"30000"` = 30 s).

### Impacto real de decisiones del operador

Tras `POST /decide`, `action_dispatcher` notifica a `sensor_producer` (`POST /control`):

| Decisión | Efecto en simulación |
| :--- | :--- |
| **APAGADO_INMEDIATO** | Sensor `SHUTDOWN` — deja de publicar en `sensor_data` |
| **IGNORAR_10_MINUTOS** | Sensor `COOLDOWN` — barra CD en dashboard, reactivación automática |
| **RECONOCER_Y_ESPERAR_24H** | `COOLDOWN` prolongado (24 h por defecto) |
| **PROGRAMAR_MANTENIMIENTO_AHORA** | `MAINTENANCE` ~45 s sin lecturas, luego vuelve a `OK` |

---

## ✅ Cumplimiento del Enunciado (Ejercicio 3)

| Requisito | Estado |
| :--- | :--- |
| Arquitectura híbrida Kafka + RabbitMQ | ✅ |
| 8 microservicios + brokers en Docker Compose | ✅ |
| Ventana móvil stateful + reglas >90 / 3×>75 | ✅ |
| Particionamiento por `sensor_id` (key) | ✅ |
| Tópico `sensor_status` consumido por `plant_monitor_backend` | ✅ |
| Fanout `human_alerts` + opciones aleatorias (orden barajado) | ✅ |
| Mensaje RabbitMQ: `{ alert_id, sensor_id, type, options }` | ✅ |
| Exchange Direct `actions_direct` para comandos | ✅ |
| Delayed Exchange: 24 h y 10 min (`x-delay`) | ✅ |
| `POST /decide` en `action_dispatcher` | ✅ |
| Dashboard: WS :8081 + :8082 + decisión al dispatcher | ✅ |

---

## 🧹 Limpiar y Detener Infraestructura

Para detener todos los servicios y remover volúmenes de red asociados:
```bash
docker compose down -v
```
