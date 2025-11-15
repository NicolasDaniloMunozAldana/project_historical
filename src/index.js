/**
 * Historical Event Sourcing Service
 * Entry point for the complaint status history consumer
 */

require('dotenv').config();
const { testConnection } = require('./config/db');
const HistoricalConsumerService = require('./services/HistoricalConsumerService');

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

    console.log('========================================');
    console.log('  Service is running successfully');
    console.log('  Listening for complaint status events');
    console.log('========================================');
    console.log('');
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
