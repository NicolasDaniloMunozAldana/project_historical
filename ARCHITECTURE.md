# Arquitectura Event Sourcing - Historial de Quejas

## Descripción General

Este documento describe la arquitectura de Event Sourcing implementada para mantener un historial inmutable de los cambios de estado de las quejas.

## Componentes

### 1. Producer (project_complaints)

**Ubicación:** `project_complaints/src/services/ComplaintStatusEventPublisher.js`

**Responsabilidades:**
- Publicar eventos de cambio de estado al topic `complaint-status-events`
- Se ejecuta cada vez que se crea o actualiza una queja
- No bloquea la operación principal (ejecución asíncrona)

**Eventos Publicados:**
```javascript
{
  id_complaint: 123,
  previous_status: "abierta",
  new_status: "en_revision",
  changed_by: "admin",
  change_description: "Estado cambiado de abierta a en_revision",
  event_timestamp: "2024-11-14T10:30:00.000Z"
}
```

### 2. Consumer (project_historical)

**Ubicación:** `project_historical/src/services/HistoricalConsumerService.js`

**Responsabilidades:**
- Consumir eventos desde el topic `complaint-status-events`
- Guardar cada evento en la tabla `historical.complaint_status_history`
- Leer desde el inicio del topic (`fromBeginning: true`)

**Características:**
- **Event Sourcing completo**: Puede reconstruir el historial desde cero
- **Procesamiento en orden**: Los eventos se procesan secuencialmente
- **Idempotencia**: Cada evento tiene un timestamp único

### 3. Base de Datos

**Schema:** `historical`

**Tabla:** `complaint_status_history`

```sql
CREATE TABLE historical.complaint_status_history (
  id_history INT PRIMARY KEY AUTO_INCREMENT,
  id_complaint INT NOT NULL,
  previous_status ENUM('abierta', 'en_revision', 'cerrada'),
  new_status ENUM('abierta', 'en_revision', 'cerrada') NOT NULL,
  changed_by VARCHAR(100),
  change_description TEXT,
  event_timestamp DATETIME NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  
  INDEX idx_complaint_id (id_complaint),
  INDEX idx_event_timestamp (event_timestamp),
  INDEX idx_new_status (new_status)
);
```

### 4. Kafka Topic

**Nombre:** `complaint-status-events`

**Configuración:**
- **Particiones:** 3 (para paralelismo)
- **Replication Factor:** 1
- **Retention:** Infinita (`retention.ms=-1`)
- **Cleanup Policy:** `compact` (mantiene el último estado de cada key)

## Flujo de Datos

```
┌─────────────────────┐
│  Usuario modifica   │
│  estado de queja    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────┐
│  ComplaintService                   │
│  - updateComplaintStatus()          │
│  - createComplaint()                │
└──────────┬──────────────────────────┘
           │
           ├──► Actualizar BD principal
           │
           ├──► Enviar email (async)
           │
           └──► Publicar evento (async)
                      │
                      ▼
           ┌──────────────────────┐
           │  Kafka Producer      │
           │  Topic:              │
           │  complaint-status-   │
           │  events              │
           └──────────┬───────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │  Kafka Consumer      │
           │  (project_historical)│
           └──────────┬───────────┘
                      │
                      ▼
           ┌──────────────────────┐
           │  historical.         │
           │  complaint_status_   │
           │  history             │
           └──────────────────────┘
```

## Ventajas de esta Arquitectura

### 1. Event Sourcing
- **Historial completo**: Cada cambio de estado queda registrado
- **Auditoría**: Se sabe quién, cuándo y por qué cambió cada estado
- **Reconstrucción**: Posibilidad de reconstruir el estado en cualquier momento

### 2. Desacoplamiento
- El sistema principal (`project_complaints`) no depende del historial
- Fallos en el consumer no afectan las operaciones de quejas
- Escalabilidad independiente

### 3. Performance
- Escrituras asíncronas al historial
- No impacto en tiempo de respuesta de la API
- Posibilidad de replay de eventos

### 4. Trazabilidad
- Cada evento tiene timestamp preciso
- Se registra el usuario que realizó el cambio
- Descripción del cambio para contexto

## Casos de Uso

### 1. Consultar Historial de una Queja

```javascript
// Obtener todos los cambios de estado de una queja
SELECT * FROM historical.complaint_status_history
WHERE id_complaint = 123
ORDER BY event_timestamp ASC;
```

### 2. Reconstruir Estado Histórico

```javascript
// Estado de una queja en una fecha específica
SELECT new_status 
FROM historical.complaint_status_history
WHERE id_complaint = 123 
  AND event_timestamp <= '2024-11-01 00:00:00'
ORDER BY event_timestamp DESC
LIMIT 1;
```

### 3. Análisis de Tiempos

```javascript
// Tiempo promedio en cada estado
SELECT 
  id_complaint,
  new_status,
  TIMESTAMPDIFF(DAY, event_timestamp, 
    LEAD(event_timestamp) OVER (PARTITION BY id_complaint ORDER BY event_timestamp)
  ) as dias_en_estado
FROM historical.complaint_status_history;
```

### 4. Auditoría

```javascript
// Cambios realizados por un usuario
SELECT * FROM historical.complaint_status_history
WHERE changed_by = 'admin'
ORDER BY event_timestamp DESC;
```

## Configuración de Kafka

### Topic Configuration

El topic `complaint-status-events` está configurado con:

- **Retention infinita**: No se eliminan eventos antiguos
- **Cleanup policy: compact**: Mantiene el último estado de cada key
- **3 particiones**: Permite procesamiento paralelo

### Consumer Group

El consumer group `historical-consumer-group` garantiza:

- **fromBeginning: true**: Lee todos los eventos desde el inicio
- **Auto-commit**: Los offsets se confirman automáticamente
- **Session timeout**: 30 segundos para detectar fallos

## Monitoring y Logs

### Producer Logs
```
[OK] Status change event published for complaint 123: abierta -> en_revision
[ERROR] Error publishing status change event: <error>
```

### Consumer Logs
```
[OK] Processing status change event for complaint 123 (partition: 0)
[OK] Event saved successfully: complaint 123 - abierta → en_revision (45ms)
[ERROR] Error processing event (120ms): <error>
```

## Consideraciones de Producción

### 1. Escalabilidad
- Incrementar particiones para mayor throughput
- Múltiples instancias del consumer (mismo group ID)

### 2. Confiabilidad
- Implementar Dead Letter Queue (DLQ) para eventos fallidos
- Monitoreo de lag del consumer
- Alertas sobre errores de procesamiento

### 3. Backup
- Backup regular del schema `historical`
- Snapshots del topic de Kafka
- Retención de logs de Kafka

### 4. Performance
- Índices en la tabla de historial
- Particionamiento de tabla por fecha si el volumen crece
- Cache de consultas frecuentes

## Diferencias con Email Notifications

| Característica | Email Notifications | Status Events |
|----------------|---------------------|---------------|
| Topic | `email-notifications` | `complaint-status-events` |
| Retention | 7 días | Infinita |
| Cleanup | Delete | Compact |
| fromBeginning | false | true |
| Propósito | Notificaciones temporales | Event Sourcing |
| Consumer | project_email_sender | project_historical |

## Integración con Redis

Ambos sistemas usan el mismo broker de Kafka configurado en `docker-compose.yml`.

La configuración de Redis para cache puede agregarse posteriormente si se requiere optimizar las consultas de historial.
