const { v4: uuidv4 } = require('uuid');
const { logWithContext } = require('../utils/logger');

/**
 * Middleware para generar y propagar correlation IDs
 * Permite rastrear una request desde el frontend hasta el consumer
 */
const correlationIdMiddleware = (req, res, next) => {
    // Intentar obtener correlation ID del header o generar uno nuevo
    const correlationId = req.headers['x-correlation-id'] || uuidv4();
    
    // Agregar correlation ID al request
    req.correlationId = correlationId;
    
    // Agregar correlation ID al response header
    res.setHeader('x-correlation-id', correlationId);
    
    // Loguear la request entrante
    logWithContext('info', 'Incoming Request', {
        correlationId,
        method: req.method,
        path: req.path,
        service: 'historical-service',
        ip: req.ip,
        userAgent: req.headers['user-agent'],
        timestamp: new Date().toISOString()
    });
    
    // Interceptar el response para loguear la salida
    const originalSend = res.send;
    res.send = function(data) {
        logWithContext('info', 'Outgoing Response', {
            correlationId,
            method: req.method,
            path: req.path,
            statusCode: res.statusCode,
            service: 'historical-service',
            timestamp: new Date().toISOString()
        });
        originalSend.call(this, data);
    };
    
    next();
};

module.exports = correlationIdMiddleware;

