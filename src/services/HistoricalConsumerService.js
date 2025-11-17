/**
 * Historical Consumer Service
 * Consumes complaint status change events from Kafka and stores them in historical database
 */

const { Kafka } = require('kafkajs');
const kafkaConfig = require('../config/kafkaConfig');
const ComplaintStatusHistory = require('../models/ComplaintStatusHistory');
const { 
  logKafkaEvent, 
  logBusinessEvent, 
  logDatabaseOperation, 
  logError,
  logger 
} = require('../utils/logger');

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
      logger.warn('Kafka is disabled');
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
      logger.info('Kafka Consumer connected successfully', {
        service: 'historical-service',
        clientId: kafkaConfig.clientId,
        brokers: kafkaConfig.brokers
      });

      // Subscribe to complaint status events topic
      // fromBeginning: true ensures we capture all historical events
      await this.consumer.subscribe({
        topic: kafkaConfig.topic,
        fromBeginning: kafkaConfig.fromBeginning,
      });

      logger.info('Subscribed to Kafka topic', {
        service: 'historical-service',
        topic: kafkaConfig.topic,
        fromBeginning: kafkaConfig.fromBeginning
      });
    } catch (error) {
      logError(error, {
        operation: 'initialize',
        service: 'historical-service'
      });
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
          await this.handleMessage(message, partition, topic);
        },
      });
      logger.info('Historical consumer started and listening for events', {
        service: 'historical-service',
        topic: kafkaConfig.topic
      });
    } catch (error) {
      logError(error, {
        operation: 'startConsuming',
        service: 'historical-service'
      });
      throw error;
    }
  }

  /**
   * Handle incoming status change event
   * @param {Object} message - Kafka message
   * @param {number} partition - Partition number
   * @param {string} topic - Kafka topic
   * @returns {Promise<void>}
   */
  async handleMessage(message, partition, topic) {
    const startTime = Date.now();
    let eventData = null;
    let correlationId = null;

    try {
      // Extract correlation ID from headers
      if (message.headers && message.headers['x-correlation-id']) {
        correlationId = message.headers['x-correlation-id'].toString();
      }

      // Parse event data
      eventData = JSON.parse(message.value.toString());

      // If no correlation ID in headers, try to get it from event data
      if (!correlationId && eventData.correlationId) {
        correlationId = eventData.correlationId;
      }

      // Extract headers
      const headers = {};
      if (message.headers) {
        Object.keys(message.headers).forEach((key) => {
          headers[key] = message.headers[key].toString();
        });
      }

      logKafkaEvent(
        'CONSUMED',
        topic,
        {
          partition,
          offset: message.offset,
          complaintId: eventData.id_complaint,
          eventType: 'Cambio de estado de queja recibido desde Kafka'
        },
        correlationId
      );

      logBusinessEvent(
        'EVENTO_CONSUMIDO_KAFKA',
        {
          topic,
          partition,
          offset: message.offset,
          complaintId: eventData.id_complaint,
          previousStatus: eventData.previous_status,
          newStatus: eventData.new_status,
          description: `Queja #${eventData.id_complaint} cambió de estado: ${eventData.previous_status || 'ninguno'} → ${eventData.new_status}`
        },
        correlationId,
        'historical-service'
      );

      // Validate event data
      if (!this.validateEventData(eventData, correlationId)) {
        throw new Error('Invalid event data structure');
      }

      // Save event to historical database
      await this.saveEventToDatabase(eventData, correlationId);

      const processingTime = Date.now() - startTime;
      logBusinessEvent(
        'EVENTO_GUARDADO_EXITOSO',
        {
          complaintId: eventData.id_complaint,
          previousStatus: eventData.previous_status || 'N/A',
          newStatus: eventData.new_status,
          processingTimeMs: processingTime,
          description: `Historial guardado exitosamente para queja #${eventData.id_complaint}. Tiempo de procesamiento: ${processingTime}ms`
        },
        correlationId,
        'historical-service'
      );
    } catch (error) {
      const processingTime = Date.now() - startTime;
      logError(error, {
        operation: 'handleMessage',
        service: 'historical-service',
        topic,
        partition,
        offset: message.offset,
        processingTimeMs: processingTime,
        eventData: eventData ? {
          id_complaint: eventData.id_complaint,
          new_status: eventData.new_status
        } : null
      }, correlationId);

      // Don't throw error to avoid blocking the consumer
      // In production, you might want to send this to a DLQ
    }
  }

  /**
   * Validate event data structure
   * @param {Object} eventData - Event data
   * @param {string} correlationId - Correlation ID for logging
   * @returns {boolean} True if valid
   */
  validateEventData(eventData, correlationId = null) {
    if (!eventData) {
      logError(new Error('Event data is null or undefined'), {
        operation: 'validateEventData',
        service: 'historical-service'
      }, correlationId);
      return false;
    }

    if (!eventData.id_complaint) {
      logError(new Error('Missing id_complaint in event data'), {
        operation: 'validateEventData',
        service: 'historical-service',
        eventData
      }, correlationId);
      return false;
    }

    if (!eventData.new_status) {
      logError(new Error('Missing new_status in event data'), {
        operation: 'validateEventData',
        service: 'historical-service',
        complaintId: eventData.id_complaint
      }, correlationId);
      return false;
    }

    const validStatuses = ['abierta', 'en_revision', 'cerrada'];
    if (!validStatuses.includes(eventData.new_status)) {
      logError(new Error(`Invalid new_status: ${eventData.new_status}`), {
        operation: 'validateEventData',
        service: 'historical-service',
        complaintId: eventData.id_complaint,
        invalidStatus: eventData.new_status
      }, correlationId);
      return false;
    }

    if (
      eventData.previous_status &&
      !validStatuses.includes(eventData.previous_status)
    ) {
      logError(new Error(`Invalid previous_status: ${eventData.previous_status}`), {
        operation: 'validateEventData',
        service: 'historical-service',
        complaintId: eventData.id_complaint,
        invalidStatus: eventData.previous_status
      }, correlationId);
      return false;
    }

    return true;
  }

  /**
   * Save event to historical database
   * @param {Object} eventData - Event data
   * @param {string} correlationId - Correlation ID for logging
   * @returns {Promise<void>}
   */
  async saveEventToDatabase(eventData, correlationId = null) {
    try {
      logDatabaseOperation(
        'INSERT',
        'historical.complaint_status_history',
        {
          complaintId: eventData.id_complaint,
          previousStatus: eventData.previous_status,
          newStatus: eventData.new_status,
          description: `Insertando registro de historial: Queja #${eventData.id_complaint} - Estado: ${eventData.previous_status || 'nuevo'} → ${eventData.new_status}`
        },
        correlationId
      );

      const historyRecord = await ComplaintStatusHistory.create({
        id_complaint: eventData.id_complaint,
        previous_status: eventData.previous_status || null,
        new_status: eventData.new_status,
        changed_by: eventData.changed_by || 'system',
        change_description: eventData.change_description || null,
        correlation_id: correlationId,
        event_timestamp: eventData.event_timestamp
          ? new Date(eventData.event_timestamp)
          : new Date(),
        created_at: new Date(),
      });

      logDatabaseOperation(
        'INSERT_SUCCESS',
        'historical.complaint_status_history',
        {
          historyId: historyRecord.id_history,
          complaintId: eventData.id_complaint,
          description: `Registro de historial #${historyRecord.id_history} creado exitosamente para queja #${eventData.id_complaint}`
        },
        correlationId
      );
    } catch (error) {
      logError(error, {
        operation: 'saveEventToDatabase',
        service: 'historical-service',
        complaintId: eventData.id_complaint
      }, correlationId);
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
        logger.info('Kafka Consumer disconnected', {
          service: 'historical-service'
        });
      } catch (error) {
        logError(error, {
          operation: 'disconnect',
          service: 'historical-service'
        });
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
