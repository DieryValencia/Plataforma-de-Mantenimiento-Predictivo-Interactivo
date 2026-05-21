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
        
        DASH[dashboard.html] -->|fetch POST /decide| DISP[action_dispatcher :3001]
        
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
   Deberías ver los 10 contenedores en estado `Up` y `healthy`.

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

> Para pruebas locales más rápidas, puedes sobreescribir en `docker-compose.yml`:
> `DELAY_24H_MS: "15000"` y `DELAY_10MIN_MS: "15000"` en el servicio `action_dispatcher`.

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
