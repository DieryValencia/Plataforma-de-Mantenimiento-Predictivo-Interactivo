# 📸 Guía de Capturas de Pantalla y Documentación Visual

## Instrucciones para Documentar la Ejecución del Proyecto

Este documento proporciona una guía paso a paso para capturar pantallas de cada componente del sistema en funcionamiento.

---

## 1. Preparación del Entorno

### Paso 1: Iniciar el Sistema
```bash
cd predictive-maintenance
docker compose down -v
docker compose up --build -d
sleep 40  # Esperar a que Kafka y RabbitMQ estén listos
```

### Paso 2: Verificar que todos los contenedores están corriendo
```bash
docker compose ps
```

**Captura esperada**: Pantalla mostrando 11 contenedores en estado "Up"
- kafka ✓
- rabbitmq ✓
- sensor_producer ✓
- alert_detector ✓
- alert_router ✓
- plant_monitor_backend ✓
- operator_console_backend ✓
- action_dispatcher ✓
- actuator_worker ✓
- maintenance_worker ✓
- notification_service ✓

---

## 2. Capturas de Terminales (Logs)

### 2.1 sensor_producer - Simulación de Sensores
```bash
docker logs -f sensor_producer | head -30
```

**Captura esperada**: 
```
[sensor_producer] 📊 Sensor A: vibration=45.3 mm/s
[sensor_producer] 📊 Sensor B: vibration=52.1 mm/s
[sensor_producer] 📊 Sensor C: vibration=48.9 mm/s
...
[sensor_producer] 📊 Sensor J: vibration=89.2 mm/s
```

**Título**: "Sensores generando datos en tiempo real - 10 sensores (A-J)"

---

### 2.2 alert_detector - Detección de Alertas
```bash
docker logs -f alert_detector | head -30
```

**Capturas esperadas**:

1. **Alerta CRÍTICA** (vibración > 90):
```
[alert_detector] 🔴 CRITICAL: Sensor A vibration=95.2 mm/s > 90
[alert_detector] 📤 Enviada a topic: alerts_critical
```

2. **Alerta WARNING** (3 lecturas > 75):
```
[alert_detector] 📊 Sensor C window: [76, 78, 77] average=77
[alert_detector] 🟡 WARNING: Sensor C average=77 > 75 (3 readings)
[alert_detector] 📤 Enviada a topic: alerts_warning
```

**Título**: "Sistema detectando alertas críticas y de advertencia"

---

### 2.3 alert_router - Puente Kafka → RabbitMQ
```bash
docker logs -f alert_router | head -20
```

**Captura esperada**:
```
[alert_router] 🔗 Consumiendo de Kafka topics: alerts_critical, alerts_warning
[alert_router] 📬 Enrutando CRITICAL → human_alerts (Opciones: APAGADO_INMEDIATO, IGNORAR_10_MINUTOS)
[alert_router] 📬 Enrutando WARNING → human_alerts (Opciones: PROGRAMAR_MANTENIMIENTO_AHORA, RECONOCER_Y_ESPERAR_24H)
```

**Título**: "Bridge automático enriqueciendo alertas de Kafka a RabbitMQ"

---

### 2.4 action_dispatcher - API de Decisiones
```bash
docker logs -f action_dispatcher | head -20
```

**Captura esperada**:
```
[action_dispatcher] 🌐 HTTP Server escuchando en puerto 3000
[action_dispatcher] ✅ Decision received: APAGADO_INMEDIATO for sensor_A
[action_dispatcher] 📤 Enrutando a critical_actions_queue
[action_dispatcher] ✅ Decision received: RECONOCER_Y_ESPERAR_24H for sensor_B
[action_dispatcher] ⏰ Programando en delayed_exchange con retraso: 15000ms
```

**Título**: "REST API procesando decisiones del operador"

---

### 2.5 notification_service - Telegram y Panel Móvil
```bash
docker logs -f notification_service | head -30
```

**Captura esperada**:
```
[notification] 📬 Cola 'notification_human_alerts' ← fanout 'human_alerts'
[notification] 🔄 Telegram polling iniciado
[notification] 📥 Encolada CRITICAL sensor A (cola: 1)
[notification] 📲 Telegram → CRITICAL sensor A
[notification] ✅ Telegram decisión: APAGADO_INMEDIATO → sensor_A
```

**Título**: "Servicio de notificaciones enviando alertas a Telegram"

---

### 2.6 actuator_worker - Ejecución de Acciones Críticas
```bash
docker logs -f actuator_worker | head -20
```

**Captura esperada**:
```
[actuator_worker] 📬 Esperando comandos en critical_actions_queue
[actuator_worker] ⚡ Recibido comando: APAGADO_INMEDIATO para sensor_A
[actuator_worker] 🛑 Simulando apagado inmediato del equipo
[actuator_worker] ✅ Acción completada y confirmada (ACK)
```

**Título**: "Worker de actuador ejecutando apagados críticos"

---

### 2.7 maintenance_worker - Tareas de Mantenimiento
```bash
docker logs -f maintenance_worker | head -20
```

**Captura esperada**:
```
[maintenance_worker] 📬 Esperando órdenes en maintenance_queue
[maintenance_worker] 🔧 Recibida orden de mantenimiento: PROGRAMAR_MANTENIMIENTO_AHORA
[maintenance_worker] ⏱️  Iniciando procedimiento de mantenimiento (duración: 45s simulada)
[maintenance_worker] ✅ Mantenimiento completado para sensor_C
```

**Título**: "Worker de mantenimiento ejecutando procedimientos"

---

## 3. Capturas de Interfaz Web

### 3.1 Acceder al Dashboard
1. Abre navegador: `http://localhost:3000`
2. Espera a que se cargue la interfaz Tailwind CSS

**Captura esperada**:
- Título: "🏭 Plataforma de Mantenimiento Predictivo"
- Grilla de 10 sensores (A-J)
- Indicadores de vibración con colores:
  - Verde (< 75): Normal
  - Amarillo (75-90): Advertencia
  - Rojo (> 90): Crítico

**Título**: "Dashboard principal con grilla de sensores en tiempo real"

---

### 3.2 Alerta Crítica Emergente
Cuando `sensor_producer` genere una lectura > 90:

**Captura esperada**:
- Panel flotante rojo con título: "🔴 CRITICAL — ACCIÓN REQUERIDA"
- Sensor ID que generó la alerta
- Valor de vibración
- Dos botones:
  - ⚡ Apagar
  - ⏭ Ignorar 10m
  - 📱 Abrir panel móvil

**Título**: "Alerta crítica emergente con opciones de acción contextuales"

---

### 3.3 Consola de Auditoría
Parte inferior del dashboard mostrando evento history:

**Captura esperada**:
```
[18:43:22] 🔴 CRITICAL: Sensor A vibration=95.2
[18:43:23] ⚡ Decision: APAGADO_INMEDIATO for sensor_A
[18:43:23] ✅ Actuator executed: Sensor A shut down
[18:43:25] 🟡 WARNING: Sensor C average=77 (3 readings)
[18:43:26] ⏳ Maintenance scheduled for 24h (15s simulated)
```

**Título**: "Consola de auditoría mostrando historial de eventos"

---

### 3.4 Panel Móvil (/m)
En otra pestaña: `http://localhost:3003/m?alert=<alert_id>`

**Captura esperada**:
- Interfaz táctil simplificada
- Botones grandes para decisiones
- Indicador de alerta actual
- Campo para descripción

**Título**: "Panel móvil táctil para operador en campo"

---

### 3.5 RabbitMQ Management
URL: `http://localhost:15672`
Credenciales: `admin` / `admin123`

**Capturas esperadas**:

1. **Tab: Exchanges**
```
Name                Type        Features
human_alerts        fanout      durable
delayed_exchange    x-delayed   durable
```

2. **Tab: Queues**
```
Name                                Messages
critical_actions_queue              0 or 1
maintenance_queue                   0 or 1
notification_human_alerts           0 to 5
```

3. **Tab: Connections**
```
Connection          State
alert_router        running
operator_console    running
notification_svc    running
```

**Título**: "RabbitMQ Management UI mostrando exchanges, queues y conexiones"

---

## 4. Flujos Completos Documentados

### Flujo 1: Alerta Crítica de Principio a Fin (5-10 segundos)

**Pasos**:
1. Captura de sensor_producer: Vibración = 95 mm/s
2. Captura de alert_detector: "🔴 CRITICAL detectada"
3. Captura de alert_router: "Enrutando a human_alerts"
4. Captura de dashboard: Alerta emergente roja
5. Captura de acción: Presionar "⚡ Apagar"
6. Captura de action_dispatcher: Decisión recibida
7. Captura de actuator_worker: Acción ejecutada
8. Captura de consola: Auditoría actualizada

**Composición visual**: Grid 2x4 mostrando cada paso

---

### Flujo 2: Alerta de Advertencia con Retraso (30+ segundos)

**Pasos**:
1. alert_detector: 3 lecturas > 75 = WARNING
2. dashboard: Alerta amarilla emergente
3. operador: Presiona "Esperar 24h"
4. action_dispatcher: Programa en delayed_exchange
5. RabbitMQ: Retraso de 15 segundos en progreso
6. maintenance_worker: Recibe tras expiración
7. consola: Auditoría muestra "Mantenimiento ejecutado"

**Composición visual**: Timeline mostrando progresión temporal

---

## 5. Instrucciones de Captura de Pantalla

### Windows
```bash
# Usar Snip & Sketch
# Atajo: Win + Shift + S

# O capturar toda la pantalla
# Atajo: Win + PrtScn
```

### macOS
```bash
# Región de pantalla
# Atajo: Cmd + Shift + 4

# Ventana específica
# Atajo: Cmd + Shift + 4, luego Espacio
```

### Linux
```bash
# Usar GNOME Screenshot
gnome-screenshot -a

# O herramienta gráfica
# Atajo: PrtScn
```

---

## 6. Organización de Carpeta de Capturas

```
capturas_ejecucion/
├── 01_docker_ps.png
│   └── Descripción: Estado de contenedores
├── 02_sensor_producer.png
│   └── Descripción: Logs generando datos
├── 03_alert_detector_critical.png
│   └── Descripción: Alerta crítica detectada
├── 04_alert_detector_warning.png
│   └── Descripción: Alerta de advertencia detectada
├── 05_alert_router.png
│   └── Descripción: Bridge enriqueciendo alertas
├── 06_action_dispatcher.png
│   └── Descripción: API procesando decisiones
├── 07_notification_service.png
│   └── Descripción: Notificaciones a Telegram
├── 08_actuator_worker.png
│   └── Descripción: Ejecución de apagado crítico
├── 09_maintenance_worker.png
│   └── Descripción: Ejecución de mantenimiento
├── 10_dashboard_principal.png
│   └── Descripción: Panel de control con sensores
├── 11_alerta_critica_emergente.png
│   └── Descripción: Pop-up de alerta con botones
├── 12_consola_auditoria.png
│   └── Descripción: Historial de eventos
├── 13_panel_movil.png
│   └── Descripción: Interfaz táctil para móvil
├── 14_rabbitmq_exchanges.png
│   └── Descripción: RabbitMQ Exchanges
├── 15_rabbitmq_queues.png
│   └── Descripción: RabbitMQ Queues
└── GUIA_VISUAL.md
    └── Documento que agrupa todas las capturas con explicaciones
```

---

## 7. Documento de Composición Visual (para el informe)

Crea `CAPTURAS_EJECUCION.md` con estructura:

```markdown
# 📸 Capturas de Ejecución del Proyecto

## 1. Inicialización del Sistema

### Docker Compose - Estado de Contenedores
![Docker PS](capturas/01_docker_ps.png)
Muestra los 11 contenedores en ejecución correcta.

## 2. Capa Kafka - Ingesta y Detección

### Sensor Producer - Simulación de 10 Sensores
![Sensor Producer](capturas/02_sensor_producer.png)
Genera 10 sensores (A-J) emitiendo vibración cada 500ms.

### Alert Detector - Detección CRÍTICA
![Alert Critical](capturas/03_alert_detector_critical.png)
Detecta cuando vibración > 90 mm/s instantáneamente.

### Alert Detector - Detección WARNING
![Alert Warning](capturas/04_alert_detector_warning.png)
Detecta cuando ventana de 3 lecturas promedia > 75 mm/s.

## 3. Capa RabbitMQ - Enrutamiento

### Alert Router - Bridge Kafka→RabbitMQ
![Alert Router](capturas/05_alert_router.png)
Enriquece alertas con opciones contextuales.

### Action Dispatcher - REST API
![Action Dispatcher](capturas/06_action_dispatcher.png)
Procesa decisiones del operador (POST /decide).

## 4. Ejecución de Acciones

### Actuator Worker - Apagado Crítico
![Actuator](capturas/08_actuator_worker.png)
Ejecuta apagado inmediato ante alerta crítica.

### Maintenance Worker - Tareas Diferidas
![Maintenance](capturas/09_maintenance_worker.png)
Ejecuta mantenimiento tras retraso de 15 segundos.

## 5. Interfaz de Usuario

### Dashboard Principal
![Dashboard](capturas/10_dashboard_principal.png)
Grilla de 10 sensores con indicadores en tiempo real.

### Alerta Emergente Crítica
![Alert Popup](capturas/11_alerta_critica_emergente.png)
Panel flotante rojo con opciones contextuales.

### Consola de Auditoría
![Console](capturas/12_consola_auditoria.png)
Historial de eventos con timestamps.

### Panel Móvil (/m)
![Mobile](capturas/13_panel_movil.png)
Interfaz táctil para operador en campo.

## 6. Infraestructura - RabbitMQ

### RabbitMQ Exchanges
![Exchanges](capturas/14_rabbitmq_exchanges.png)
Muestra human_alerts (fanout) y delayed_exchange.

### RabbitMQ Queues
![Queues](capturas/15_rabbitmq_queues.png)
Muestra critical_actions_queue, maintenance_queue, etc.

---

## Nota
Todas las capturas se ejecutaron en un entorno Docker con:
- Kafka 7.5.0 (KRaft)
- RabbitMQ Latest
- Node.js 18 (Alpine)
- Docker Compose v2.0+
```

---

## 8. Verificación de Funcionalidades por Captura

| # | Componente | Funcionalidad | Evidencia en Captura |
|---|-----------|--------------|---------------------|
| 1 | docker ps | 11 contenedores corriendo | Estados "Up" para todos |
| 2 | sensor_producer | 10 sensores generando datos | Logs mostrando A-J con vibración |
| 3 | alert_detector | Regla crítica > 90 | 🔴 CRITICAL detectada |
| 4 | alert_detector | Regla warning 3x > 75 | 🟡 WARNING promedio=77 |
| 5 | alert_router | Bridge Kafka→RabbitMQ | "Enrutando a human_alerts" |
| 6 | action_dispatcher | REST API POST /decide | "Decision received" + enrutamiento |
| 7 | notification_service | Telegram polling | "Telegram polling iniciado" |
| 8 | actuator_worker | Apagado inmediato | "Simulando apagado" + ACK |
| 9 | maintenance_worker | Tareas diferidas | "Mantenimiento completado" |
| 10 | dashboard | Grilla de sensores | 10 cajas con vibración en vivo |
| 11 | dashboard | Alerta emergente | Panel rojo con botones contextuales |
| 12 | dashboard | Consola de auditoría | Timeline de eventos |
| 13 | notification_service | Panel móvil /m | Interfaz táctil simplificada |
| 14 | rabbitmq | Exchanges (Fanout + Delayed) | 2 exchanges visibles en Management |
| 15 | rabbitmq | Queues | 3+ queues visibles con mensajes |

---

## 9. Script de Automatización (Opcional)

Crea `capturar_ejecucion.sh`:

```bash
#!/bin/bash

# Crear carpeta
mkdir -p capturas_ejecucion

# Capturar logs de cada contenedor
echo "Capturando logs..."
docker logs sensor_producer > capturas_ejecucion/01_sensor_producer.log
docker logs alert_detector > capturas_ejecucion/02_alert_detector.log
docker logs alert_router > capturas_ejecucion/03_alert_router.log
docker logs action_dispatcher > capturas_ejecucion/04_action_dispatcher.log
docker logs notification_service > capturas_ejecucion/05_notification_service.log
docker logs actuator_worker > capturas_ejecucion/06_actuator_worker.log
docker logs maintenance_worker > capturas_ejecucion/07_maintenance_worker.log

# Capturar estado del sistema
docker ps > capturas_ejecucion/08_docker_ps.log

echo "✅ Logs capturados en capturas_ejecucion/"
```

---

## 10. Recomendaciones Finales

✅ **Captura de al menos 15 pantallas/logs** para documentar:
- Sistema en ejecución
- Flujo CRÍTICO completo
- Flujo WARNING completo
- Interface web
- Infraestructura (RabbitMQ)

✅ **Incluye timestamps** en cada captura para demostrar latencias reales

✅ **Anota observaciones** en cada captura (número de sensores, valores, etc.)

✅ **Evidencia de interacción humana** (clicks en botones, decisiones)

✅ **Proof of concept**: Mostrar que el sistema reacciona en tiempo real

---

**Nota**: Este documento complementa el README_TECNICO.md y proporciona evidencia visual de que todas las funcionalidades están implementadas y operativas.
