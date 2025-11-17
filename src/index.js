/**
 * Historical Event Sourcing Service
 * Entry point for the complaint status history consumer
 */

require('dotenv').config();
const express = require('express');
const { testConnection } = require('./config/db');
const HistoricalConsumerService = require('./services/HistoricalConsumerService');
const correlationIdMiddleware = require('./middlewares/correlationId');
const logsController = require('./controllers/logsController');

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3033;

// Middleware
app.use(express.json());
app.use(correlationIdMiddleware);

// Logs viewer interface
app.get('/logs', (req, res) => logsController.renderLogsViewer(req, res));

// Logs API routes
app.get('/api/logs/files', (req, res) => logsController.getLogFiles(req, res));
app.get('/api/logs/business', (req, res) => logsController.getBusinessLogs(req, res));
app.get('/api/logs/:filename', (req, res) => logsController.getLogs(req, res));
app.get('/api/logs/search/:correlationId', (req, res) => logsController.searchByCorrelationId(req, res));
app.get('/api/logs/errors/recent', (req, res) => logsController.getRecentErrors(req, res));

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'historical-service',
    timestamp: new Date().toISOString(),
    kafka: {
      enabled: process.env.KAFKA_ENABLED === 'true',
      connected: consumerService?.isConsumerConnected() || false,
    },
  });
});

// Service status endpoint
app.get('/status', (req, res) => {
  res.json({
    service: 'Historical Service',
    version: '1.0.0',
    uptime: process.uptime(),
    kafka: {
      enabled: process.env.KAFKA_ENABLED === 'true',
      brokers: process.env.KAFKA_BROKERS,
      topic: process.env.KAFKA_TOPIC_COMPLAINT_STATUS_CHANGE,
      groupId: process.env.KAFKA_GROUP_ID,
      connected: consumerService?.isConsumerConnected() || false,
    },
    database: {
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
    },
  });
});

// Initialize consumer service
const consumerService = new HistoricalConsumerService();

/**
 * Start the historical consumer service
 */
async function startService() {
  console.log('========================================');
  console.log('  Historical Event Sourcing Service');
  console.log('========================================');
  console.log('');

  try {
    // Test database connection
    console.log('[INFO] Testing database connection...');
    await testConnection();
    console.log('');

    // Initialize and start Kafka consumer
    console.log('[INFO] Initializing Kafka consumer...');
    await consumerService.initialize();
    console.log('');

    console.log('[INFO] Starting event consumption...');
    await consumerService.startConsuming();
    console.log('');

    // Start Express server
    app.listen(PORT, () => {
      console.log('========================================');
      console.log('  Service is running successfully');
      console.log('  Listening for complaint status events');
      console.log(`  HTTP Server: http://localhost:${PORT}`);
      console.log(`  Health check: http://localhost:${PORT}/health`);
      console.log(`  Status: http://localhost:${PORT}/status`);
      console.log(`  Logs API: http://localhost:${PORT}/api/logs/files`);
      console.log('========================================');
      console.log('');
    });
  } catch (error) {
    console.error('[FATAL] Failed to start service:', error.message);
    process.exit(1);
  }
}

/**
 * Graceful shutdown handler
 */
async function gracefulShutdown(signal) {
  console.log('');
  console.log(`[INFO] ${signal} received, shutting down gracefully...`);

  try {
    await consumerService.disconnect();
    console.log('[OK] Service stopped successfully');
    process.exit(0);
  } catch (error) {
    console.error('[ERROR] Error during shutdown:', error.message);
    process.exit(1);
  }
}

// Handle shutdown signals
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('[FATAL] Uncaught Exception:', error.message);
  console.error(error.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] Unhandled Rejection at:', promise);
  console.error('[FATAL] Reason:', reason);
  process.exit(1);
});

// Start the service
startService();
