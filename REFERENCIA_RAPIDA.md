# 🔗 Referencia Rápida - URLs, Endpoints y Comandos

## 📍 URLs de Acceso Rápido

| Servicio | URL | Descripción |
|----------|-----|-------------|
| **Dashboard Principal** | http://localhost:3000 | Consola de operaciones web |
| **Panel Móvil** | http://localhost:3003/m | Interfaz táctil para móvil |
| **RabbitMQ Management** | http://localhost:15672 | Monitoreo de mensajería |
| **Kafka (Bootstrap)** | localhost:9092 | Broker para clientes externos |

---

## 🎯 WebSocket Endpoints (Tiempo Real)

| Endpoint | URL | Descripción | Cliente |
|----------|-----|-------------|---------|
| **Plant Monitor** | ws://localhost:8081 | Telemetría de sensores | dashboard.html |
| **Operator Console** | ws://localhost:8082 | Alertas para operador | dashboard.html |

---

## 🔌 REST API Endpoints

### Servidor Principal: `http://localhost:3001`

#### POST /decide
**Descripción**: Enviar decisión de acción desde operador

**Headers**:
```
Content-Type: application/json
```

**Body**:
```json
{
  "alert_id": "7c41045e-ab19-4e49-801b-e22eef2592e4",
  "sensor_id": "sensor_A",
  "chosen_action": "APAGADO_INMEDIATO"
}
```

**Response** (200 OK):
```json
{
  "success": true,
  "message": "Action queued for execution",
  "action": "APAGADO_INMEDIATO",
  "enrouted_to": "critical_actions_queue"
}
```

**Acciones válidas**:
- `APAGADO_INMEDIATO` → critical_actions_queue
- `IGNORAR_10_MINUTOS` → (solo auditoría)
- `PROGRAMAR_MANTENIMIENTO_AHORA` → maintenance_queue
- `RECONOCER_Y_ESPERAR_24H` → delayed_exchange (15s)

---

## 📬 RabbitMQ Configuration

### Acceso Management UI
- **URL**: http://localhost:15672
- **Usuario**: admin
- **Contraseña**: admin123

### Credenciales AMQP
- **Usuario**: admin
- **Contraseña**: admin123
- **Host**: rabbitmq
- **Puerto**: 5672

### Exchanges
| Nombre | Tipo | Descripción |
|--------|------|-------------|
| `human_alerts` | Fanout | Distribuye alertas a múltiples operadores |
| `delayed_exchange` | x-delayed-message | Tareas diferidas (24h simulado = 15s) |

### Queues
| Nombre | Fuente | Destino | Contenido |
|--------|--------|---------|-----------|
| `critical_actions_queue` | action_dispatcher | actuator_worker | Apagados inmediatos |
| `maintenance_queue` | action_dispatcher | maintenance_worker | Mantenimiento inmediato |
| `notification_human_alerts` | human_alerts | notification_service | Alertas para notificación |
| `operator_console_queue` | human_alerts | operator_console_backend | Alertas para dashboard |

---

## 🐳 Comandos Docker

### Gestión General
```bash
# Navegar al directorio del proyecto
cd predictive-maintenance

# Ver estado de todos los contenedores
docker compose ps

# Iniciar sistema (construir e iniciar)
docker compose up --build -d

# Detener sistema
docker compose down

# Limpiar todo (volúmenes incluidos)
docker compose down -v

# Reiniciar servicios específicos
docker compose restart notification_service
docker compose restart action_dispatcher
```

### Ver Logs
```bash
# Logs de todos los servicios
docker compose logs -f

# Logs de servicio específico (últimas 20 líneas)
docker logs sensor_producer --tail 20

# Logs en tiempo real
docker compose logs -f alert_detector

# Exportar logs a archivo
docker logs sensor_producer > sensor_logs.txt
```

### Ejecutar Comandos en Contenedores
```bash
# Acceder a shell del contenedor
docker exec -it sensor_producer sh

# Ejecutar comando específico
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Inspeccionar RabbitMQ
docker exec -it rabbitmq rabbitmqctl list_exchanges
docker exec -it rabbitmq rabbitmqctl list_queues
```

---

## 🔍 Kafka Commands

### Ver Tópicos
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

### Crear Tópico (si fuera necesario)
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic nombre_topico --partitions 3 --replication-factor 1
```

### Consumir Mensajes (desde el principio)
```bash
# Tópico: sensor_data
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor_data --from-beginning --max-messages 10

# Tópico: alerts_critical
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic alerts_critical --from-beginning

# Tópico: alerts_warning
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic alerts_warning --from-beginning
```

### Monitorear en Tiempo Real
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic sensor_data --from-latest
```

---

## 🧪 Pruebas Manuales

### Test 1: Generar Alerta Crítica Manualmente
```bash
# Conectar a Kafka y enviar dato
docker exec kafka kafka-console-producer --bootstrap-server localhost:29092 \
  --topic sensor_data --property "parse.key=true" --property "key.separator=:"

# Pegar esto:
sensor_A:{"sensor_id":"sensor_A","vibration":95.5,"timestamp":"2026-05-21T18:00:00Z"}

# Presionar Ctrl+C para salir
```

### Test 2: Test de REST API
```bash
# Usando PowerShell (Windows)
$body = @{
  alert_id = "test-alert-123"
  sensor_id = "sensor_B"
  chosen_action = "APAGADO_INMEDIATO"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:3001/decide" `
  -Method POST `
  -ContentType "application/json" `
  -Body $body
```

### Test 3: Test de WebSocket
```bash
# Terminal 1: Monitorear plant_monitor
docker logs -f plant_monitor_backend

# Terminal 2: Monitorear operator_console
docker logs -f operator_console_backend

# Verificar en dashboard que ambos están conectados
```

---

## 📊 Métricas y Monitoreo

### Health Check Manual
```bash
# Verificar que todos los servicios respondan
echo "Kafka:" && nc -zv localhost 9092
echo "RabbitMQ AMQP:" && nc -zv localhost 5672
echo "RabbitMQ Web:" && curl -I http://localhost:15672
echo "Plant Monitor WS:" && curl -I http://localhost:8081
echo "Operator Console WS:" && curl -I http://localhost:8082
echo "Action Dispatcher:" && curl -I http://localhost:3001
echo "Notification Service:" && curl -I http://localhost:3003
```

### Ver Tráfico de Mensajes
```bash
# Terminal 1: Ver sensor_producer
docker logs -f sensor_producer

# Terminal 2: Ver alert_detector
docker logs -f alert_detector

# Terminal 3: Ver RabbitMQ en Management UI
# Acceder a http://localhost:15672 y ver:
# - Messages in/out por queue
# - Conexiones activas
# - Publicaciones por segundo
```

### Métricas en Tiempo Real
```bash
# Ver consumo de recursos
docker stats

# Ver detalles de red
docker network inspect predictive-maintenance_iot-network

# Ver información de volúmenes
docker volume ls | grep predictive-maintenance
```

---

## 🔧 Troubleshooting Rápido

### Problema: "Port already in use"
```bash
# Windows - Ver qué proceso usa el puerto
netstat -ano | findstr :3001
# Resultado: TCP    0.0.0.0:3001            0.0.0.0:0              LISTENING    PID
taskkill /PID <PID> /F

# Linux/Mac
lsof -i :3001
kill -9 <PID>
```

### Problema: Kafka no inicia
```bash
# Ver logs detallados
docker logs -f kafka

# Esperar 30-40 segundos y verificar health
docker ps | grep kafka
# Debe mostrar "(healthy)"

# Si falla: reintentar
docker compose restart kafka
```

### Problema: RabbitMQ no conecta
```bash
# Ver estado
docker exec rabbitmq rabbitmqctl status

# Reiniciar
docker compose restart rabbitmq

# Ver logs
docker logs -f rabbitmq
```

### Problema: WebSocket no conecta desde browser
```bash
# Verificar que los puertos estén abiertos
netstat -an | find "8081"
netstat -an | find "8082"

# Abrir DevTools del navegador (F12)
# Ver consola para errores de conexión
# Tipicamente: "WebSocket connection to 'ws://localhost:8081' failed"

# Solución: Reiniciar servicios
docker compose restart plant_monitor_backend operator_console_backend
```

### Problema: Telegram no recibe mensajes
```bash
# Ver logs de notification_service
docker logs -f notification_service

# Buscar errores como:
# "[notification] ❌ Telegram poll: Bad Request"

# Verificar configuración en .env
cat notification_service/.env | grep TELEGRAM

# Prueba directa de token
$token = "tu_token_aqui"
$url = "https://api.telegram.org/bot$token/getMe"
(Invoke-RestMethod -Uri $url).result
```

---

## 📝 Variables de Entorno Importantes

### notification_service/.env
```env
# Telegram
TELEGRAM_BOT_TOKEN=8993332814:AAEnoQzmwAJmmUF9VHe4S9lcVTXUcMa5NvE
TELEGRAM_CHAT_ID=1216488595

# Notificaciones
NOTIFY_CRITICAL_ONLY=true
NOTIFY_WARNINGS=false

# URLs
RABBITMQ_URL=amqp://admin:admin123@rabbitmq:5672
DECIDE_URL=http://action_dispatcher:3000/decide
PUBLIC_BASE_URL=http://localhost:3003
HTTP_PORT=3003
```

### docker-compose.yml (Credenciales)
```yaml
RABBITMQ_DEFAULT_USER: admin
RABBITMQ_DEFAULT_PASS: admin123
KAFKA_BROKER: kafka:29092
```

---

## 📈 Performance Tips

### Optimizar Latencia
```bash
# Reducir retraso de Kafka (si aplica)
# En alert_detector: Aumentar frecuencia de polling

# Reducir retraso de RabbitMQ
# En docker-compose: Aumentar prefetch_count

# Monitorear latencias
watch -n 1 'docker stats --no-stream'
```

### Escalar Horizontalmente
```bash
# Aumentar réplicas de servicio
docker compose up --scale sensor_producer=2 -d

# Aumentar particiones de Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --topic sensor_data --alter --partitions 6
```

---

## 🎯 Comandos Frecuentes

| Acción | Comando |
|--------|---------|
| Iniciar sistema | `docker compose up --build -d` |
| Ver todos los logs | `docker compose logs -f` |
| Detener todo | `docker compose down` |
| Limpiar todo | `docker compose down -v` |
| Ver estado | `docker compose ps` |
| Reiniciar servicio | `docker compose restart <servicio>` |
| Ver logs de servicio | `docker logs -f <contenedor>` |
| Ver topicos Kafka | `docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list` |
| Acceder a RabbitMQ | http://localhost:15672 (admin/admin123) |
| Ver métricas | `docker stats` |
| Ejecutar en contenedor | `docker exec -it <contenedor> <comando>` |

---

## 🔒 Seguridad

### Cambiar Credenciales
```bash
# En docker-compose.yml
RABBITMQ_DEFAULT_USER: nuevo_usuario
RABBITMQ_DEFAULT_PASS: nueva_contraseña_fuerte

# En notification_service/.env
TELEGRAM_BOT_TOKEN=tu_nuevo_token
TELEGRAM_CHAT_ID=tu_nuevo_chat_id
```

### Habilitar TLS (Producción)
```yaml
# Agregar a docker-compose.yml
environment:
  RABBITMQ_SSL_CACERTFILE: /etc/ssl/certs/ca.pem
  RABBITMQ_SSL_CERTFILE: /etc/ssl/certs/server.pem
  RABBITMQ_SSL_KEYFILE: /etc/ssl/certs/server-key.pem
```

---

## 📞 Contacto Rápido

**GitHub**: https://github.com/DieryValencia/Plataforma-de-Mantenimiento-Predictivo-Interactivo

**Documentación**:
- README_TECNICO.md - Documentación completa
- SUMARIO_EJECUTIVO.md - Resumen ejecutivo
- GUIA_CAPTURAS.md - Evidencia visual

---

**Última actualización**: 21 de Mayo de 2026  
**Versión**: 1.0.0
