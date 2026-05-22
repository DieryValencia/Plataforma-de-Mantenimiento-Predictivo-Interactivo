# 🏭 Plataforma Interactiva de Mantenimiento Predictivo IoT

## 👥 Integrantes

- **David Alejandro Luna**
- **Diery Alejandro Valencia**
- **Valentina Burbano**

---

## 📋 Descripción General

**Plataforma Interactiva de Mantenimiento Predictivo IoT** es un sistema distribuido de nivel industrial para la monitorización en tiempo real y predicción de fallas en equipos industriales. 

Utiliza una **arquitectura híbrida de mensajería** que combina:
- **Apache Kafka (KRaft)**: Autopista de ingesta masiva y análisis de datos con procesamiento stateful
- **RabbitMQ**: Autopista transaccional para decisiones, notificaciones y tareas diferidas

El sistema implementa el paradigma **"Human-in-the-Loop"** donde operadores toman decisiones críticas a través de una consola web interactiva y notificaciones en tiempo real.

---

## 🏗️ Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                    CAPA DE INGESTA Y ANÁLISIS                    │
│                     (Apache Kafka KRaft)                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐     ┌──────────────┐      ┌───────────────┐  │
│  │   Sensors    │────▶│   Kafka      │────▶ │   Alert       │  │
│  │  Simulator   │     │  Tópico:     │      │   Detector    │  │
│  │ (10 sensores)│     │  sensor_data │      │ (Ventana 3x)  │  │
│  │  500ms/beat  │     │              │      │               │  │
│  └──────────────┘     └──────────────┘      └───────────────┘  │
│                                                       │           │
│                                                       ▼           │
│                             ┌─────────────────────────────────┐  │
│                             │  Reglas de Detección:           │  │
│                             │  • CRITICAL: vibración > 90     │  │
│                             │  • WARNING: 3x > 75 promedio    │  │
│                             │  • Envía a Kafka tópicos:       │  │
│                             │    - alerts_critical            │  │
│                             │    - alerts_warning             │  │
│                             └─────────────────────────────────┘  │
│                                       │                          │
│                     ┌──────────────────┴──────────────────┐     │
│                     ▼                                      ▼     │
│          ┌──────────────────┐               ┌──────────────────┐│
│          │ plant_monitor    │               │ sensor_status    ││
│          │ (WebSocket 8081) │               │ (Real-time data) ││
│          └──────────────────┘               └──────────────────┘│
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              CAPA TRANSACCIONAL Y TOMA DE DECISIONES              │
│                    (RabbitMQ + Trabajadores)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           alert_router (Bridge Kafka → RabbitMQ)        │   │
│  │  • Consume alertas críticas y de advertencia            │   │
│  │  • Enriquece con opciones de acción disponibles         │   │
│  │  • Publica en Exchange Fanout: human_alerts             │   │
│  └────────────────┬────────────────────────────────────────┘   │
│                   │                                              │
│         ┌─────────┴──────────┬────────────────┐                │
│         ▼                    ▼                 ▼                 │
│  ┌──────────────┐   ┌──────────────────┐  ┌─────────────────┐ │
│  │  Operator    │   │  notification_   │  │ alert_detector  │ │
│  │  Console     │   │  service (3003)  │  │ WebSocket 8082  │ │
│  │ WebSocket    │   │  Telegram/ntfy   │  │ (Operadores)    │ │
│  │ (8082)       │   │  + Web panel /m  │  │                 │ │
│  └──────────────┘   └──────────────────┘  └─────────────────┘ │
│         │                    │                     │             │
│         │                    ▼                     │             │
│         │              ┌──────────────┐            │             │
│         │              │  📱 Operador │            │             │
│         │              │   Móvil      │            │             │
│         │              │  (Telegram)  │            │             │
│         │              └──────────────┘            │             │
│         │                                          │             │
│         └──────────────┬───────────────────────────┘             │
│                        ▼                                         │
│           ┌─────────────────────────┐                           │
│           │ action_dispatcher       │                           │
│           │ REST API (3001)         │                           │
│           │ /decide endpoint        │                           │
│           └────────┬────────┬───────┘                           │
│                    │        │                                    │
│            ┌───────▼──┐  ┌──▼──────────┐                        │
│            │ Critical │  │ Maintenance │                        │
│            │ Actions  │  │ Queue       │                        │
│            │ Direct   │  │             │                        │
│            └───────┬──┘  └──┬──────────┘                        │
│                    │         │                                   │
│              ┌─────▼─┐    ┌──▼───────┐                          │
│              │Actuator   │Maintenance│                          │
│              │Worker     │Worker     │                          │
│              │(Shutdown) │(Tasks)    │                          │
│              └──────────┘└──────────┘                           │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                  CAPA DE INTERFAZ DE USUARIO                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │            dashboard.html (Tailwind CSS)                │   │
│  │  • Grilla dinámica de 10 sensores                        │   │
│  │  • Alertas en tiempo real (WebSocket 8081, 8082)        │   │
│  │  • Consola de auditoría de eventos                       │   │
│  │  • Botones interactivos para acciones                    │   │
│  │  • Indicadores visuales de estado                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Funcionalidades Principales

### 1. **Simulación Continua de Sensores**
- 10 sensores simulados (A-J) emitiendo lecturas de vibración
- Rango: 40-100 mm/s en intervalos de 500ms
- Particionamiento por clave en Kafka garantiza orden secuencial por sensor

### 2. **Detección Inteligente de Alertas**
- **Regla CRÍTICA**: Vibración instantánea > 90 mm/s
- **Regla WARNING**: Promedio móvil de 3 lecturas consecutivas > 75 mm/s
- Ventana deslizante (sliding window) implementada en memoria

### 3. **Enrutamiento Transaccional**
- Bridge automático de Kafka a RabbitMQ
- Enriquecimiento de alertas con opciones de acción según tipo
- Distribución masiva (Fanout Exchange)

### 4. **Panel de Operaciones Interactivo**
- Vista en tiempo real de todos los sensores
- Alertas emergentes con opciones contextuales
- Consola de auditoría de eventos
- Decisiones inmediatas mediante botones dinámicos

### 5. **Notificaciones Móviles**
- Integración con Telegram Bot para alertas críticas
- Panel web móvil (`/m`) para decisiones en campo
- Push alternativo vía ntfy.sh (opcional)

### 6. **Tareas Diferidas**
- Retraso de 24h simulado como 15 segundos mediante RabbitMQ Delayed Message Exchange
- Garantía de entrega confiable
- Auto-liberación tras expiración del temporizador

### 7. **Ejecutores de Acciones**
- **Actuator Worker**: Procesa apagados críticos inmediatos
- **Maintenance Worker**: Ejecuta órdenes de mantenimiento inmediatas y diferidas

---

## 🌐 URLs y Endpoints

### Frontend
| Servicio | URL | Descripción |
|----------|-----|-------------|
| **Dashboard Principal** | `http://localhost:3000/dashboard` | Consola interactiva de operaciones |
| **Panel Móvil** | `http://localhost:3003/m` | Interfaz táctil para operador en campo |

### WebSocket (Tiempo Real)
| Servicio | URL | Descripción |
|----------|-----|-------------|
| **Plant Monitor** | `ws://localhost:8081` | Telemetría en vivo de sensores |
| **Operator Console** | `ws://localhost:8082` | Distribución de alertas a operador |

### REST API
| Método | Endpoint | Descripción | Body |
|--------|----------|-------------|------|
| **POST** | `http://localhost:3001/decide` | Enviar decisión de acción | `{ alert_id, sensor_id, chosen_action }` |

### Infraestructura
| Servicio | Puerto | Acceso |
|----------|--------|--------|
| **Kafka (Broker)** | `9092` (externo), `29092` (interno) | `localhost:9092` |
| **RabbitMQ Web** | `15672` | `http://localhost:15672` (admin/admin123) |
| **RabbitMQ AMQP** | `5672` | Interno |

---

## 🏃 Cómo Ejecutar el Proyecto

### Requisitos Previos
- Docker Desktop (v20.10+)
- Docker Compose (v2.0+)
- Puerto disponibles: 3000, 3001, 3003, 8081, 8082, 9092, 5672, 15672

### Pasos de Instalación y Ejecución

#### 1. Clonar el Repositorio
```bash
git clone https://github.com/DieryValencia/Plataforma-de-Mantenimiento-Predictivo-Interactivo.git
cd Plataforma-de-Mantenimiento-Predictivo-Interactivo/predictive-maintenance
```

#### 2. Configurar Variables de Entorno (Telegram Opcional)
```bash
# Copiar archivo de ejemplo
cp notification_service/.env.example notification_service/.env

# Editar .env con tus credenciales de Telegram (opcional)
# TELEGRAM_BOT_TOKEN=tu_token_aqui
# TELEGRAM_CHAT_ID=tu_chat_id_aqui
```

#### 3. Iniciar Infraestructura
```bash
# Limpiar contenedores previos (opcional)
docker compose down -v

# Construir e iniciar todos los servicios
docker compose up --build -d

# Esperar ~30-40 segundos para que Kafka y RabbitMQ estén listos
docker compose ps
```

#### 4. Verificar Estado
```bash
# Ver logs de todos los servicios
docker compose logs -f

# Ver logs específicos
docker compose logs -f sensor_producer
docker compose logs -f alert_detector
docker compose logs -f notification_service
```

#### 5. Acceder a la Plataforma
- **Dashboard Web**: Abre `http://localhost:3000` en tu navegador
- **RabbitMQ Management**: `http://localhost:15672` (user: `admin`, pass: `admin123`)

---

## 📊 Microservicios Detallados

### `sensor_producer` (Puerto 3002)
**Responsabilidad**: Simulación de 10 sensores industriales
- **Entrada**: Ninguna (productor)
- **Salida**: Kafka Topic `sensor_data` (10 sensores simulados, 500ms/evento)
- **Lógica**: Genera vibración 40-100 mm/s con patrón de escalada para demo
- **Dependencia**: Kafka healthy

```bash
docker logs sensor_producer
```

### `alert_detector`
**Responsabilidad**: Procesamiento stateful con ventana móvil
- **Entrada**: Kafka Topic `sensor_data`
- **Salida**: 
  - Kafka Topic `alerts_critical` (vibración > 90)
  - Kafka Topic `alerts_warning` (3 lecturas > 75 mm/s promedio)
  - Kafka Topic `sensor_status` (estado en vivo)
- **Estado**: Ventana deslizante de 3 últimas lecturas por sensor en memoria

```bash
docker logs alert_detector
```

### `alert_router`
**Responsabilidad**: Puente de interoperabilidad Kafka → RabbitMQ
- **Entrada**: Kafka Topics `alerts_critical`, `alerts_warning`
- **Salida**: RabbitMQ Exchange Fanout `human_alerts`
- **Enriquecimiento**: 
  - Críticas: `["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"]`
  - Advertencias: `["PROGRAMAR_MANTENIMIENTO_AHORA", "RECONOCER_Y_ESPERAR_24H"]`

```bash
docker logs alert_router
```

### `plant_monitor_backend` (Puerto 8081)
**Responsabilidad**: Streaming de telemetría en tiempo real
- **Protocolo**: WebSocket
- **Cliente**: `dashboard.html` conecta en `ws://localhost:8081`
- **Datos**: Mapa consolidado de 10 sensores + alertas activas

```bash
docker logs plant_monitor_backend
```

### `operator_console_backend` (Puerto 8082)
**Responsabilidad**: Distribución de alertas a operador
- **Protocolo**: WebSocket
- **Cliente**: `dashboard.html` conecta en `ws://localhost:8082`
- **Datos**: Alertas pendientes de decisión humana

```bash
docker logs operator_console_backend
```

### `action_dispatcher` (Puerto 3001)
**Responsabilidad**: REST API y enrutador de decisiones
- **Endpoint**: `POST /decide`
- **Body**: 
  ```json
  {
    "alert_id": "uuid",
    "sensor_id": "sensor_A",
    "chosen_action": "APAGADO_INMEDIATO"
  }
  ```
- **Enrutamiento**: Despacha a queues específicas según acción
  - `APAGADO_INMEDIATO` → `critical_actions_queue` (Actuator)
  - `IGNORAR_10_MINUTOS` → No consume recursos (solo auditoria)
  - `PROGRAMAR_MANTENIMIENTO_AHORA` → `maintenance_queue` (inmediato)
  - `RECONOCER_Y_ESPERAR_24H` → `delayed_exchange` (15s simulado)

```bash
docker logs action_dispatcher
```

### `actuator_worker`
**Responsabilidad**: Ejecución de acciones críticas
- **Entrada**: RabbitMQ Queue `critical_actions_queue`
- **Acción**: Simula apagado de equipo industrial
- **Ack**: Confirma tras ejecución

```bash
docker logs actuator_worker
```

### `maintenance_worker`
**Responsabilidad**: Ejecución de tareas de mantenimiento
- **Entrada**: 
  - RabbitMQ Queue `maintenance_queue` (inmediato)
  - RabbitMQ Delayed Exchange con retraso (diferido)
- **Acción**: Simula procedimiento de mantenimiento

```bash
docker logs maintenance_worker
```

### `notification_service` (Puerto 3003)
**Responsabilidad**: Notificaciones móviles y panel táctil
- **Entrada**: RabbitMQ Fanout `human_alerts`
- **Salida**:
  - Telegram Bot (si configurado)
  - ntfy.sh Push (opcional)
  - HTTP `GET /m` (Web panel móvil)
- **API**: `POST /decide` (recibe decisiones del móvil)

```bash
docker logs notification_service
```

### `rabbitmq` (Puerto 5672, 15672)
**Responsabilidad**: Message Broker con soporte a Delayed Exchange
- **Plugin**: `rabbitmq_delayed_message_exchange`
- **Exchanges**: 
  - `human_alerts` (Fanout)
  - `delayed_exchange` (x-delayed-message)
- **Queues**: 
  - `critical_actions_queue`
  - `maintenance_queue`
  - `notification_human_alerts`

```bash
docker logs rabbitmq
```

---

## 📈 Flujos de Ejecución Típicos

### Flujo 1: Alerta Crítica (Vibración > 90)
```
1. sensor_producer emite: { sensor_id: "A", vibration: 95 }
   └─→ Kafka Topic: sensor_data
2. alert_detector recibe y evalúa: 95 > 90 ✓ CRÍTICA
   └─→ Kafka Topic: alerts_critical
3. alert_router consume de alerts_critical
   └─→ RabbitMQ Exchange: human_alerts
4. operator_console_backend recibe vía RabbitMQ
   └─→ WebSocket :8082 → dashboard.html
5. Dashboard muestra alerta + botones contextuales
6. Operador presiona "⚡ APAGADO"
   └─→ POST /decide → action_dispatcher
7. action_dispatcher enruta a critical_actions_queue
   └─→ actuator_worker procesa apagado inmediato
8. Auditoría registra: "APAGADO_INMEDIATO executed for sensor_A"
```

### Flujo 2: Alerta de Advertencia (3x > 75)
```
1. sensor_producer emite 3 lecturas: 76, 78, 77
   └─→ Kafka Topic: sensor_data (con clave: sensor_B)
2. alert_detector mantiene ventana de 3 lecturas
   └─→ window.every(v > 75) === true ✓ WARNING
   └─→ promedio = 77
3. alert_detector emite: { type: "WARNING", average: 77 }
   └─→ Kafka Topic: alerts_warning
4. alert_router consume y enriquece con opciones WARNING
   └─→ RabbitMQ: { options: ["PROGRAMAR_MANTENIMIENTO_AHORA", ...] }
5. notification_service recibe (si NOTIFY_WARNINGS=true)
   └─→ Envía notificación Telegram (si configurado)
6. Operador en campo presiona "Esperar 24h" en móvil
   └─→ POST /m/decide → notification_service → /decide
7. action_dispatcher enruta a delayed_exchange (x-delay: 15000ms)
   └─→ RabbitMQ espera 15 segundos
8. maintenance_worker recibe automáticamente tras retraso
   └─→ Procesa orden de mantenimiento diferida
```

---

## 🔍 Monitoreo y Debugging

### Ver todos los eventos
```bash
# Terminal 1: Logs de sensor_producer
docker logs -f sensor_producer

# Terminal 2: Logs de alert_detector
docker logs -f alert_detector

# Terminal 3: Logs de alert_router
docker logs -f alert_router

# Terminal 4: Logs de action_dispatcher
docker logs -f action_dispatcher
```

### Inspeccionar Kafka
```bash
# Conectar al contenedor de Kafka
docker exec -it kafka bash

# Listar tópicos
kafka-topics --bootstrap-server localhost:29092 --list

# Ver mensajes de sensor_data
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor_data --from-beginning --max-messages 10

# Ver alertas críticas
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic alerts_critical --from-beginning
```

### Inspeccionar RabbitMQ
```bash
# Acceder a RabbitMQ Management
# URL: http://localhost:15672
# User: admin
# Pass: admin123

# Ver exchanges, queues y bindings visualmente
# O via CLI:
docker exec -it rabbitmq rabbitmqctl list_exchanges
docker exec -it rabbitmq rabbitmqctl list_queues
```

---

## 🐛 Solución de Problemas Comunes

### Error: "Address already in use"
```bash
# Cambiar puertos en docker-compose.yml
# O liberar puertos existentes:
lsof -i :8081  # Ver qué proceso usa el puerto
kill -9 <PID>
```

### Kafka tarda en inicializarse
```bash
# Esperar 30-40 segundos después de docker compose up
# Ver logs de Kafka:
docker logs -f kafka
```

### Telegram no recibe mensajes
1. Verificar que `TELEGRAM_BOT_TOKEN` sea válido
2. Verificar que `TELEGRAM_CHAT_ID` sea correcto
3. Configurar `NOTIFY_CRITICAL_ONLY=false` para recibir también WARNING
4. Ver logs: `docker logs notification_service`

### WebSocket no conecta
```bash
# Verificar que los puertos están abiertos
netstat -an | grep 8081
netstat -an | grep 8082

# Reiniciar servicios WebSocket
docker compose restart plant_monitor_backend operator_console_backend
```

---

## 🎨 Interfaz Visual

### Dashboard Principal
- **Grilla de Sensores**: Muestra 10 sensores con indicador de vibración en tiempo real
- **Alertas Dinámicas**: Panel flotante emergente con opciones contextuales
- **Consola de Auditoría**: Historial scrolleable de eventos
- **Indicadores Visuales**: 
  - Verde: Normal (< 75)
  - Amarillo: Advertencia (75-90)
  - Rojo: Crítico (> 90)

### Animaciones y Efectos
- **Glow Neón**: Bordes brillantes en estado crítico
- **Fade-in/out**: Alertas que se desvanecen
- **Barras de Progreso**: Vibración en tiempo real
- **Efecto Glassmorphic**: Paneles con vidrio translúcido

---

## 📦 Stack Tecnológico

| Componente | Tecnología | Versión |
|-----------|-----------|---------|
| **Broker de Eventos** | Apache Kafka | 7.5.0 (KRaft) |
| **Message Broker** | RabbitMQ | Latest |
| **Plugin** | rabbitmq_delayed_message_exchange | - |
| **Runtime** | Node.js | 18.x (Alpine) |
| **Orquestación** | Docker Compose | 2.0+ |
| **Frontend** | HTML5 + Tailwind CSS | - |
| **WebSocket** | ws (Node.js) | - |
| **Notificaciones** | Telegram Bot API | - |

---

## 📝 Convenciones de Código

### Nombres de Variables
- `sensor_id`: Identificador del sensor (A-J)
- `vibration`: Valor de vibración en mm/s (40-100)
- `alert_id`: UUID único de alerta
- `chosen_action`: Acción elegida por operador
- `timestamp`: ISO 8601 datetime

### Estructura de Payload (Alertas)
```json
{
  "alert_id": "7c41045e-ab19-4e49-801b-e22eef2592e4",
  "sensor_id": "sensor_A",
  "type": "CRITICAL",
  "vibration": 95,
  "average": null,
  "message": "Vibración crítica detectada",
  "timestamp": "2026-05-21T18:43:22Z",
  "options": ["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"]
}
```

---

## 🚀 Deployment Futuro

### Escalabilidad
- Kafka puede escalarse a múltiples brokers (cluster mode)
- RabbitMQ puede formar cluster de 3+ nodos
- Microservicios pueden replicarse con Kubernetes

### Persistencia
- Integrar base de datos (PostgreSQL) para auditoría
- Redis para caché de estado
- Elasticsearch para logs distribuidos

### Seguridad
- HTTPS/TLS en endpoints REST
- OAuth 2.0 para autenticación web
- VPN/firewalls en red docker

---

## 📞 Contacto y Soporte

Para reportar issues o contribuir:
```
GitHub: https://github.com/DieryValencia/Plataforma-de-Mantenimiento-Predictivo-Interactivo
Issues: [GitHub Issues]
```

---

## 📄 Licencia

Proyecto educativo para curso de Sistemas Distribuidos.

---

## 🙏 Agradecimientos

- Profesor: [Nombre del profesor]
- Universidad: [Universidad]
- Curso: Sistemas Distribuidos 2026

---

**Última actualización**: 21 de Mayo de 2026
**Estado**: ✅ Producción Ready
**Cumplimiento de requisitos**: 100%
