/**
 * Kafka Configuration for Historical Consumer
 * Configuration for consuming complaint status change events
 * All values are configurable via environment variables
 */

require('dotenv').config();

/**
 * Helper function to parse integer from environment variable
 * @param {string} key - Environment variable key
 * @param {number} defaultValue - Default value if not set
 * @returns {number} Parsed integer value
 */
const getEnvInt = (key, defaultValue) => {
  const value = process.env[key];
  return value ? parseInt(value, 10) : defaultValue;
};

/**
 * Helper function to parse float from environment variable
 * @param {string} key - Environment variable key
 * @param {number} defaultValue - Default value if not set
 * @returns {number} Parsed float value
 */
const getEnvFloat = (key, defaultValue) => {
  const value = process.env[key];
  return value ? parseFloat(value) : defaultValue;
};

/**
 * Helper function to parse boolean from environment variable
 * @param {string} key - Environment variable key
 * @param {boolean} defaultValue - Default value if not set
 * @returns {boolean} Parsed boolean value
 */
const getEnvBool = (key, defaultValue) => {
  const value = process.env[key];
  if (value === undefined) return defaultValue;
  return value === 'true' || value === '1';
};

module.exports = {
  // Kafka Broker Connection Settings
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),

  // Client ID for identification
  clientId: process.env.KAFKA_CLIENT_ID || 'historical-service',

  // Connection settings
  connectionTimeout: getEnvInt('KAFKA_CONNECTION_TIMEOUT', 10000),
  requestTimeout: getEnvInt('KAFKA_REQUEST_TIMEOUT', 30000),

  // Consumer Settings
  consumer: {
    allowAutoTopicCreation: getEnvBool('KAFKA_ALLOW_AUTO_TOPIC_CREATION', false),
    groupId: process.env.KAFKA_GROUP_ID || 'historical-consumer-group',
    sessionTimeout: getEnvInt('KAFKA_SESSION_TIMEOUT', 30000),
    rebalanceTimeout: getEnvInt('KAFKA_REBALANCE_TIMEOUT', 60000),
    heartbeatInterval: getEnvInt('KAFKA_HEARTBEAT_INTERVAL', 3000),
  },

  // Topic Definition
  topic: process.env.KAFKA_TOPIC_COMPLAINT_STATUS_EVENTS || 'complaint-status-events',

  // Event Sourcing Settings - Read from beginning to capture all events
  fromBeginning: getEnvBool('KAFKA_FROM_BEGINNING', true),
  autoCommit: getEnvBool('KAFKA_AUTO_COMMIT', true),
  autoCommitInterval: getEnvInt('KAFKA_AUTO_COMMIT_INTERVAL', 5000),

  // Retry Policy
  retries: {
    maxAttempts: getEnvInt('KAFKA_MAX_RETRIES', 3),
    initialRetryTime: getEnvInt('KAFKA_INITIAL_RETRY_TIME', 100),
    maxRetryTime: getEnvInt('KAFKA_MAX_RETRY_TIME', 30000),
    multiplier: getEnvFloat('KAFKA_RETRY_MULTIPLIER', 2),
  },

  // Enable/Disable Kafka
  enabled: getEnvBool('KAFKA_ENABLED', true),
};
