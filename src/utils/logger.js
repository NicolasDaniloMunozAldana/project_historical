const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const path = require('path');

// Definir formato personalizado
const logFormat = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.json()
);

// Configuración de rotación de archivos
const dailyRotateFileTransport = new DailyRotateFile({
    filename: path.join('logs', 'application-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    maxSize: '20m',
    maxFiles: '14d',
    level: 'info'
});

const errorRotateFileTransport = new DailyRotateFile({
    filename: path.join('logs', 'error-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    maxSize: '20m',
    maxFiles: '30d',
    level: 'error'
});

// Crear logger
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: logFormat,
    transports: [
        dailyRotateFileTransport,
        errorRotateFileTransport,
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple()
            )
        })
    ]
});

/**
 * Helper para crear logs estructurados con contexto
 * @param {string} level - Nivel del log (info, warn, error)
 * @param {string} message - Mensaje del log
 * @param {Object} meta - Metadata adicional
 */
const logWithContext = (level, message, meta = {}) => {
    const logEntry = {
        message,
        timestamp: new Date().toISOString(),
        ...meta
    };
    
    logger.log(level, logEntry);
};

/**
 * Logger para eventos del sistema
 */
const logEvent = (eventType, eventData, correlationId = null) => {
    logWithContext('info', `EVENT: ${eventType}`, {
        eventType,
        correlationId,
        data: eventData,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para errores con contexto completo
 */
const logError = (error, context = {}, correlationId = null) => {
    logWithContext('error', error.message, {
        correlationId,
        error: {
            message: error.message,
            stack: error.stack,
            name: error.name
        },
        context,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para operaciones de base de datos
 */
const logDatabaseOperation = (operation, table, details = {}, correlationId = null) => {
    logWithContext('info', `DB Operation: ${operation} on ${table}`, {
        operation,
        table,
        correlationId,
        details,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para operaciones del producer/consumer
 */
const logMessageQueue = (action, messageType, messageData = {}, correlationId = null) => {
    logWithContext('info', `Queue ${action}: ${messageType}`, {
        action, // 'PRODUCED' o 'CONSUMED'
        messageType,
        correlationId,
        messageData,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para event sourcing
 */
const logEventSourcing = (eventType, aggregateId, eventData, correlationId = null) => {
    logWithContext('info', `Event Sourcing: ${eventType}`, {
        eventType,
        aggregateId,
        correlationId,
        eventData,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger específico para eventos de negocio (trazabilidad)
 * Incluye información estructurada para seguimiento end-to-end
 */
const logBusinessEvent = (eventType, eventData, correlationId = null, service = 'historical-service') => {
    logWithContext('info', `BUSINESS_EVENT: ${eventType}`, {
        eventType,
        correlationId,
        service,
        businessEvent: true, // Flag para identificar eventos de negocio
        eventData: {
            ...eventData,
            timestamp: new Date().toISOString()
        },
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para eventos de Kafka (producer/consumer)
 */
const logKafkaEvent = (action, topic, eventData, correlationId = null) => {
    logWithContext('info', `Kafka ${action}: ${topic}`, {
        action, // 'PRODUCED' o 'CONSUMED'
        topic,
        correlationId,
        kafkaEvent: true,
        eventData,
        timestamp: new Date().toISOString()
    });
};

/**
 * Logger para comunicación HTTP entre microservicios
 */
const logMicroserviceCall = (action, service, endpoint, eventData, correlationId = null) => {
    logWithContext('info', `Microservice ${action}: ${service}${endpoint}`, {
        action, // 'CALL' o 'RESPONSE'
        service,
        endpoint,
        correlationId,
        microserviceCall: true,
        eventData,
        timestamp: new Date().toISOString()
    });
};

module.exports = {
    logger,
    logWithContext,
    logEvent,
    logError,
    logDatabaseOperation,
    logMessageQueue,
    logEventSourcing,
    logBusinessEvent,
    logKafkaEvent,
    logMicroserviceCall
};

