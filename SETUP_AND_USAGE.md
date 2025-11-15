# Historical Service - Configuración y Uso

## 📋 Descripción

Este servicio consume eventos de cambios de estado de quejas desde Kafka y los almacena en un esquema histórico de la base de datos para Event Sourcing.

## 🔧 Configuración Inicial

### Prerrequisitos

1. Node.js (v14 o superior)
2. MySQL corriendo en localhost:3306
3. Base de datos `dbcomplaints` creada
4. **Kafka broker corriendo desde `project_email_sender`**

### Instalación

```bash
cd project_historical
npm install
```

### Variables de Entorno

Copia `.env.example` a `.env` y ajusta si es necesario:

```bash
cp .env.example .env
```

Variables importantes:
- `KAFKA_BROKERS=localhost:9092` - Conecta al mismo broker que `project_email_sender`
- `KAFKA_TOPIC_COMPLAINT_STATUS_EVENTS=complaint-status-events` - Topic de eventos de estado
- `KAFKA_FROM_BEGINNING=true` - Lee todos los eventos desde el inicio (Event Sourcing)
- `DB_NAME=dbcomplaints` - Misma base de datos que `project_complaints`

## 🚀 Cómo Usar

### Paso 1: Levantar Kafka

**IMPORTANTE:** Kafka debe levantarse desde `project_email_sender`, NO desde este proyecto.

```bash
cd ../project_email_sender
docker-compose up -d
```

Espera a que Kafka esté saludable (30-40 segundos):

```bash
docker-compose logs kafka-init-topics
```

Deberías ver:
```
[SUCCESS] All topics created successfully
[INFO] Available topics:
complaint-status-events
email-dlq
email-notifications
```

### Paso 2: Verificar que los topics existen

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Paso 3: Levantar el Historical Service

```bash
cd ../project_historical
npm start
```

Deberías ver:
```
[OK] MySQL database connection established successfully
[OK] Historical schema and table initialized
[OK] Kafka consumer connected successfully
[OK] Starting to consume from complaint-status-events (from beginning: true)
[INFO] Historical Consumer Service is running...
```

## 🧪 Cómo Probar

### Opción 1: Generar eventos reales desde project_complaints

1. Levanta `project_complaints`:
```bash
cd ../project_complaints
npm start
```

2. Crea una queja desde la interfaz web (http://localhost:3001)

3. Actualiza el estado de la queja (en proceso, resuelta, etc.)

4. Observa los logs de `project_historical` - deberías ver:
```
[EVENT] Processing status change event: complaint #1
[OK] Status change recorded in history: abierta -> en proceso
```

5. Verifica en la base de datos:
```sql
SELECT * FROM historical.complaint_status_history ORDER BY event_timestamp DESC;
```

### Opción 2: Enviar eventos de prueba manualmente

Desde otra terminal, envía un evento de prueba:

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic complaint-status-events
```

Luego pega este JSON y presiona Enter:
```json
{"id_complaint":999,"previous_status":"abierta","new_status":"en proceso","changed_by":"admin","change_description":"Prueba manual","event_timestamp":"2025-11-14T10:00:00.000Z"}
```

Verifica en los logs de `project_historical` que se procesó el evento.

## 📊 Consultas Útiles

### Ver todos los cambios de estado históricos
```sql
SELECT * FROM historical.complaint_status_history 
ORDER BY event_timestamp DESC 
LIMIT 10;
```

### Ver historial de una queja específica
```sql
SELECT * FROM historical.complaint_status_history 
WHERE id_complaint = 1 
ORDER BY event_timestamp ASC;
```

### Contar eventos procesados por día
```sql
SELECT DATE(event_timestamp) as fecha, COUNT(*) as total_eventos
FROM historical.complaint_status_history
GROUP BY DATE(event_timestamp)
ORDER BY fecha DESC;
```

## 🔍 Verificación de Arquitectura

### Arquitectura Actual

```
┌─────────────────────┐
│ project_complaints  │
│   (Producer)        │
└──────────┬──────────┘
           │ Publica eventos
           ▼
    ┌──────────────┐
    │    Kafka     │◄─────────────────────┐
    │   Broker     │                      │
    │ (localhost:  │                      │
    │    9092)     │                      │
    └──────┬───────┘                      │
           │                              │
           ├─────────────────┐            │
           │                 │            │
           ▼                 ▼            │
  ┌─────────────────┐ ┌──────────────────┴──┐
  │project_email    │ │ project_historical  │
  │   _sender       │ │   (Consumer)        │
  │ (Consumer)      │ │                     │
  └─────────────────┘ └─────────────────────┘
           │                     │
           ▼                     ▼
    ┌───────────┐         ┌────────────┐
    │   Gmail   │         │   MySQL    │
    │           │         │ historical │
    └───────────┘         │   schema   │
                          └────────────┘
```

### Verificar que todo funciona

1. **Kafka corriendo:**
```bash
docker ps | grep kafka
```

2. **Topics creados:**
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

3. **Historical service consumiendo:**
```bash
# Deberías ver logs en la terminal de project_historical
```

4. **Datos en la base:**
```sql
SELECT COUNT(*) FROM historical.complaint_status_history;
```

## ⚠️ Troubleshooting

### Error: "Connection refused to localhost:9092"
- Verifica que Kafka esté corriendo: `docker ps | grep kafka`
- Si no está corriendo, ve a `project_email_sender` y ejecuta `docker-compose up -d`

### Error: "Topic does not exist"
- Verifica que los topics existan: `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`
- Si falta `complaint-status-events`, reinicia Kafka: `cd project_email_sender && docker-compose restart`

### No se procesan eventos
- Verifica que `project_complaints` tenga `KAFKA_ENABLED=true` en su `.env`
- Revisa los logs de Kafka: `docker-compose logs kafka`
- Verifica conectividad: `telnet localhost 9092`

### Eventos duplicados
- Es normal si reinicias el servicio con `KAFKA_FROM_BEGINNING=true`
- Para evitarlo en producción, usa `KAFKA_FROM_BEGINNING=false` después de la primera ejecución

## 🛑 Cómo Detener

1. Detener Historical Service: `Ctrl+C` en la terminal

2. Detener Kafka (desde project_email_sender):
```bash
cd ../project_email_sender
docker-compose down
```

## 📝 Notas Importantes

- **Un solo broker Kafka:** Tanto `project_email_sender` como `project_historical` usan el mismo broker
- **Event Sourcing:** El historical service puede reproducir TODOS los eventos desde el inicio
- **Schema separado:** Los datos históricos están en el schema `historical`, no en el schema público
- **Compactación:** El topic usa `cleanup.policy=compact` para mantener el último estado de cada queja indefinidamente
