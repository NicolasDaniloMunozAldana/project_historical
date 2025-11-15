/**
 * Historical Consumer Service
 * Consumes complaint status change events from Kafka and stores them in historical database
 */

const { Kafka } = require('kafkajs');
const kafkaConfig = require('../config/kafkaConfig');
const ComplaintStatusHistory = require('../models/ComplaintStatusHistory');

class HistoricalConsumerService {
  constructor() {
    this.kafka = null;
    this.consumer = null;
    this.isConnected = false;
  }

  /**
   * Initialize Kafka consumer for event sourcing
   * @returns {Promise<void>}
   */
  async initialize() {
    if (!kafkaConfig.enabled) {
      console.log('[WARN] Kafka is disabled');
      return;
    }

    try {
      this.kafka = new Kafka({
        clientId: kafkaConfig.clientId,
        brokers: kafkaConfig.brokers,
        connectionTimeout: kafkaConfig.connectionTimeout,
        requestTimeout: kafkaConfig.requestTimeout,
        retry: {
          initialRetryTime: kafkaConfig.retries.initialRetryTime,
          retries: kafkaConfig.retries.maxAttempts,
          maxRetryTime: kafkaConfig.retries.maxRetryTime,
          multiplier: kafkaConfig.retries.multiplier,
        },
      });

      this.consumer = this.kafka.consumer(kafkaConfig.consumer);
      await this.consumer.connect();
      this.isConnected = true;
      console.log('[OK] Kafka Consumer connected successfully');

      // Subscribe to complaint status events topic
      // fromBeginning: true ensures we capture all historical events
      await this.consumer.subscribe({
        topic: kafkaConfig.topic,
        fromBeginning: kafkaConfig.fromBeginning,
      });

      console.log(
        `[OK] Subscribed to topic: ${kafkaConfig.topic} (fromBeginning: ${kafkaConfig.fromBeginning})`
      );
    } catch (error) {
      console.error('[ERROR] Failed to connect Kafka Consumer:', error.message);
      this.isConnected = false;
      throw error;
    }
  }

  /**
   * Start consuming and processing status change events
   * @returns {Promise<void>}
   */
  async startConsuming() {
    if (!this.isConnected) {
      throw new Error('Consumer not connected. Call initialize() first.');
    }

    try {
      await this.consumer.run({
        autoCommit: kafkaConfig.autoCommit,
        autoCommitInterval: kafkaConfig.autoCommitInterval,
        eachMessage: async ({ topic, partition, message }) => {
          await this.handleMessage(message, partition);
        },
      });
      console.log('[OK] Historical consumer started and listening for events');
    } catch (error) {
      console.error('[ERROR] Error in consumer loop:', error.message);
      throw error;
    }
  }

  /**
   * Handle incoming status change event
   * @param {Object} message - Kafka message
   * @param {number} partition - Partition number
   * @returns {Promise<void>}
   */
  async handleMessage(message, partition) {
    const startTime = Date.now();
    let eventData = null;

    try {
      // Parse event data
      eventData = JSON.parse(message.value.toString());

      // Extract headers
      const headers = {};
      if (message.headers) {
        Object.keys(message.headers).forEach((key) => {
          headers[key] = message.headers[key].toString();
        });
      }

      console.log(
        `[OK] Processing status change event for complaint ${eventData.id_complaint} (partition: ${partition})`
      );

      // Validate event data
      if (!this.validateEventData(eventData)) {
        throw new Error('Invalid event data structure');
      }

      // Save event to historical database
      await this.saveEventToDatabase(eventData);

      const processingTime = Date.now() - startTime;
      console.log(
        `[OK] Event saved successfully: complaint ${eventData.id_complaint} - ${eventData.previous_status || 'N/A'} → ${eventData.new_status} (${processingTime}ms)`
      );
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error(
        `[ERROR] Error processing event (${processingTime}ms):`,
        error.message
      );

      // Log failed event for manual review
      if (eventData) {
        console.error('[ERROR] Failed event data:', JSON.stringify(eventData));
      }

      // Don't throw error to avoid blocking the consumer
      // In production, you might want to send this to a DLQ
    }
  }

  /**
   * Validate event data structure
   * @param {Object} eventData - Event data
   * @returns {boolean} True if valid
   */
  validateEventData(eventData) {
    if (!eventData) {
      console.error('[ERROR] Event data is null or undefined');
      return false;
    }

    if (!eventData.id_complaint) {
      console.error('[ERROR] Missing id_complaint in event data');
      return false;
    }

    if (!eventData.new_status) {
      console.error('[ERROR] Missing new_status in event data');
      return false;
    }

    const validStatuses = ['abierta', 'en_revision', 'cerrada'];
    if (!validStatuses.includes(eventData.new_status)) {
      console.error(`[ERROR] Invalid new_status: ${eventData.new_status}`);
      return false;
    }

    if (
      eventData.previous_status &&
      !validStatuses.includes(eventData.previous_status)
    ) {
      console.error(
        `[ERROR] Invalid previous_status: ${eventData.previous_status}`
      );
      return false;
    }

    return true;
  }

  /**
   * Save event to historical database
   * @param {Object} eventData - Event data
   * @returns {Promise<void>}
   */
  async saveEventToDatabase(eventData) {
    try {
      await ComplaintStatusHistory.create({
        id_complaint: eventData.id_complaint,
        previous_status: eventData.previous_status || null,
        new_status: eventData.new_status,
        changed_by: eventData.changed_by || 'system',
        change_description: eventData.change_description || null,
        event_timestamp: eventData.event_timestamp
          ? new Date(eventData.event_timestamp)
          : new Date(),
        created_at: new Date(),
      });
    } catch (error) {
      console.error('[ERROR] Error saving to database:', error.message);
      throw error;
    }
  }

  /**
   * Disconnect the consumer
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (this.consumer && this.isConnected) {
      try {
        await this.consumer.disconnect();
        this.isConnected = false;
        console.log('[OK] Kafka Consumer disconnected');
      } catch (error) {
        console.error(
          '[ERROR] Error disconnecting Kafka Consumer:',
          error.message
        );
        throw error;
      }
    }
  }

  /**
   * Check if consumer is connected
   * @returns {boolean}
   */
  isConsumerConnected() {
    return this.isConnected;
  }
}

module.exports = HistoricalConsumerService;
