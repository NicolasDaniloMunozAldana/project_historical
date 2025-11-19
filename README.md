# Project Historical - Event Sourcing Service

This project is a Kafka consumer service for historical tracking of complaint status changes, implementing Event Sourcing patterns for immutable audit trails and complete state reconstruction.

**Current Version:** 1.0.0

## Main Features

- **Event Sourcing**: Complete, immutable history of all complaint status changes
- **Kafka Consumer**: Consumes events from `complaint-status-events` topic
- **Historical Database**: Stores events in `historical.complaint_status_history` table
- **Event Replay**: Reads from beginning of topic for complete historical reconstruction
- **Correlation ID Tracking**: End-to-end traceability across microservices
- **Structured Logging**: Winston with daily log rotation and JSON formatting
- **Auto-commit**: Automatic offset management with configurable intervals
- **Testing**: Comprehensive test suite with 44 tests using Jest
- **Retry Policy**: Configurable retry mechanism for failed operations

## Event Sourcing Architecture

The project implements **Event Sourcing** to maintain a complete, immutable history of all complaint status changes. Every time a complaint's status changes in the main application, an event is published to Kafka and consumed by this service.

### Architecture Components

- **Producer**: `project_complaints` publishes status change events to Kafka
- **Topic**: `complaint-status-events` with infinite retention and compact cleanup policy
- **Consumer**: This service (`project_historical`) consumes and stores events
- **Database**: `historical.complaint_status_history` table stores the immutable event log
- **Replay**: Consumer can replay all events from the beginning (`fromBeginning: true`)

### Event Structure

Each event includes:
- `id_complaint`: Complaint ID
- `previous_status`: Previous status (abierta, en_revision, cerrada)
- `new_status`: New status
- `changed_by`: User who made the change
- `change_description`: Optional description of the change
- `event_timestamp`: When the event occurred
- `correlation_id`: Unique identifier for end-to-end tracing
- `created_at`: When the event was stored in the database

### Benefits

- **Complete audit trail**: Know exactly when and by whom each status change was made
- **Historical reconstruction**: Rebuild the state of any complaint at any point in time
- **Temporal queries**: Analyze how long complaints stayed in each status
- **Event replay**: Recover from failures by replaying all events
- **Immutable log**: Events are never modified or deleted
- **Decoupling**: Main application continues working even if historical service is down

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed documentation.

## Logging and Traceability

This service implements comprehensive logging with end-to-end traceability using **Correlation IDs** and **Winston**. Each request is tracked from the frontend through multiple microservices.

### Documentation

- **[LOGGING_TRACEABILITY.md](./LOGGING_TRACEABILITY.md)** - Complete logging and traceability guide
- **[IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md)** - Implementation summary and setup guide

### Key Features

- **Correlation IDs**: Unique identifiers propagated through Kafka headers and payload
- **Structured Logging**: JSON logs with context, timestamps, and correlation IDs
- **Auto-rotation**: Daily log files with automatic cleanup (14 days for info, 30 days for errors)
- **Event Tracking**: Logs for Kafka consumption, database operations, and event sourcing
- **Error Handling**: Comprehensive error logging without blocking the consumer

### Log Levels

- `info`: General operations (connection, event processing, database operations)
- `error`: Errors with full context and stack traces
- Logs are stored in `logs/application-YYYY-MM-DD.log` and `logs/error-YYYY-MM-DD.log`

### Searching Logs

```bash
# Search by correlation ID
grep "abc-123-def-456" logs/application-*.log

# Parse JSON logs
grep "abc-123-def-456" logs/application-*.log | jq '.'

# Search in database
SELECT * FROM historical.complaint_status_history 
WHERE correlation_id = 'abc-123-def-456';
```

## Project Structure

```
project_historical/
├── src/
│   ├── index.js                              # Entry point
│   ├── config/
│   │   ├── db.js                             # Sequelize database configuration
│   │   └── kafkaConfig.js                    # Kafka consumer configuration
│   ├── models/
│   │   └── ComplaintStatusHistory.js         # Event history data model
│   ├── services/
│   │   ├── HistoricalConsumerService.js      # Kafka consumer service
│   │   └── HistoricalConsumerService.test.js # Service tests (38 tests)
│   ├── middlewares/
│   │   ├── correlationId.js                  # Correlation ID middleware
│   │   └── correlationId.test.js             # Middleware tests (6 tests)
│   └── utils/
│       └── logger.js                          # Winston logger configuration
├── logs/                                      # Application logs (auto-rotated)
├── migrations/
│   └── add_correlation_id.sql                # Database migration
├── ARCHITECTURE.md                            # Event Sourcing architecture docs
├── LOGGING_TRACEABILITY.md                    # Logging and traceability guide
├── IMPLEMENTATION_SUMMARY.md                  # Implementation summary
├── jest.config.js                             # Jest test configuration
├── .env.example                               # Environment variables template
├── .gitignore
├── package.json
└── README.md
```

## Authors

- **Luis Enrique Hernández Valbuena** - [@Luisen1](https://github.com/Luisen1)
- **Kevin Johann Jimenez Poveda** - [@KevP2051](https://github.com/KevP2051)
- **Nicolas Danilo Muñoz Aldana** - [@NicolasDaniloMunozAldana](https://github.com/NicolasDaniloMunozAldana)

## License

ISC
