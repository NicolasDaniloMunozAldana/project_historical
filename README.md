# Historical Event Sourcing Service

Este servicio consume eventos de cambios de estado de quejas desde Kafka y los almacena en una tabla histórica para Event Sourcing.

## Descripción

El servicio `project_historical` es un consumidor de Kafka que:

- Escucha eventos de cambios de estado de quejas desde el topic `complaint-status-events`
- Almacena cada evento en la tabla `historical.complaint_status_history`
- Permite recorrer el historial completo desde el inicio (`fromBeginning: true`)
- Mantiene un registro inmutable de todos los cambios de estado

## Requisitos

- Node.js 18+
- MySQL 8.0+
- Kafka (disponible en el mismo broker que `project_email_sender`)
- Acceso a la base de datos con el schema `historical` creado

## Instalación

```bash
npm install
```

## Configuración

Crear archivo `.env` con las siguientes variables:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=root
DB_NAME=complaints_db
DB_DIALECT=mysql

# Kafka Configuration
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=historical-service
KAFKA_GROUP_ID=historical-consumer-group
KAFKA_TOPIC_COMPLAINT_STATUS_EVENTS=complaint-status-events

# Connection Settings
KAFKA_CONNECTION_TIMEOUT=10000
KAFKA_REQUEST_TIMEOUT=30000
KAFKA_SESSION_TIMEOUT=30000
KAFKA_REBALANCE_TIMEOUT=60000
KAFKA_HEARTBEAT_INTERVAL=3000

# Consumer Settings
KAFKA_FROM_BEGINNING=true
KAFKA_AUTO_COMMIT=true
KAFKA_AUTO_COMMIT_INTERVAL=5000

# Retry Policy
KAFKA_MAX_RETRIES=3
KAFKA_INITIAL_RETRY_TIME=100
KAFKA_MAX_RETRY_TIME=30000
KAFKA_RETRY_MULTIPLIER=2

# Logging
LOG_LEVEL=info
```

## Uso

### Modo Producción

```bash
npm start
```

### Modo Desarrollo

```bash
npm run dev
```

## Arquitectura

### Event Sourcing

El servicio implementa Event Sourcing almacenando cada cambio de estado como un evento inmutable:

- **id_history**: Identificador único del evento
- **id_complaint**: ID de la queja relacionada
- **previous_status**: Estado anterior de la queja
- **new_status**: Nuevo estado de la queja
- **changed_by**: Usuario que realizó el cambio
- **change_description**: Descripción del cambio
- **event_timestamp**: Fecha y hora del evento
- **created_at**: Fecha de registro en la BD

### Consumo de Eventos

El consumidor Kafka está configurado para:

- Leer desde el principio del topic (`fromBeginning: true`)
- Procesar eventos en orden secuencial
- Guardar cada evento en la base de datos histórica
- Hacer commit automático de offsets

## Estructura del Proyecto

```
project_historical/
├── src/
│   ├── config/
│   │   ├── db.js                    # Configuración de Sequelize
│   │   └── kafkaConfig.js           # Configuración de Kafka
│   ├── models/
│   │   └── ComplaintStatusHistory.js # Modelo de historial
│   ├── services/
│   │   └── HistoricalConsumerService.js # Consumer de Kafka
│   └── index.js                     # Punto de entrada
├── .env                             # Variables de entorno
├── .gitignore
├── package.json
└── README.md
```

## Integración con Docker

Este servicio utiliza el mismo broker de Kafka definido en `project_email_sender`:

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    # ... configuración existente
```

## Logs

El servicio genera logs para:

- ✅ `[OK]` Conexión exitosa a Kafka
- ✅ `[OK]` Eventos procesados correctamente
- ⚠️ `[WARN]` Advertencias de configuración
- ❌ `[ERROR]` Errores en el procesamiento

## Notas Importantes

1. **Schema Histórico**: Asegúrate de que el schema `historical` existe en la base de datos
2. **Topic de Kafka**: El topic `complaint-status-events` debe estar creado antes de iniciar el servicio
3. **FromBeginning**: El consumer lee desde el inicio para garantizar que no se pierdan eventos
4. **Idempotencia**: Los eventos se almacenan con timestamp único para evitar duplicados
