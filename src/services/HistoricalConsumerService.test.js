/**
 * Tests for HistoricalConsumerService
 * Tests business logic for event sourcing of complaint status changes
 */

const HistoricalConsumerService = require('./HistoricalConsumerService');
const ComplaintStatusHistory = require('../models/ComplaintStatusHistory');
const kafkaConfig = require('../config/kafkaConfig');

// Mock dependencies
jest.mock('kafkajs');
jest.mock('../models/ComplaintStatusHistory');
jest.mock('../config/kafkaConfig');

describe('HistoricalConsumerService - Business Logic Tests', () => {
  let service;
  let mockConsumer;
  let mockKafka;

  beforeEach(() => {
    // Reset all mocks before each test
    jest.clearAllMocks();

    // Mock Kafka consumer
    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };

    // Mock Kafka instance
    mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer),
    };

    // Mock kafkajs Kafka constructor
    const { Kafka } = require('kafkajs');
    Kafka.mockImplementation(() => mockKafka);

    // Default kafka config
    kafkaConfig.enabled = true;
    kafkaConfig.clientId = 'test-client';
    kafkaConfig.brokers = ['localhost:9092'];
    kafkaConfig.topic = 'complaint-status-events';
    kafkaConfig.fromBeginning = true;
    kafkaConfig.consumer = { groupId: 'test-group' };
    kafkaConfig.connectionTimeout = 3000;
    kafkaConfig.requestTimeout = 3000;
    kafkaConfig.autoCommit = true;
    kafkaConfig.autoCommitInterval = 5000;
    kafkaConfig.retries = {
      initialRetryTime: 100,
      maxAttempts: 5,
      maxRetryTime: 30000,
      multiplier: 2,
    };

    // Mock ComplaintStatusHistory
    ComplaintStatusHistory.create = jest.fn().mockResolvedValue({
      id_history: 1,
      id_complaint: 123,
    });

    service = new HistoricalConsumerService();
  });

  // ==================== TEST 1: Validación de datos del evento ====================
  describe('Test 1: validateEventData - Validación de estructura de eventos', () => {
    test('Debe validar correctamente un evento completo y válido', () => {
      const validEvent = {
        id_complaint: 123,
        previous_status: 'abierta',
        new_status: 'en_revision',
        changed_by: 'admin',
        change_description: 'Revisando queja',
        event_timestamp: '2024-11-14T10:30:00.000Z',
      };

      const result = service.validateEventData(validEvent);
      expect(result).toBe(true);
    });

    test('Debe rechazar evento sin id_complaint', () => {
      const invalidEvent = {
        new_status: 'en_revision',
        changed_by: 'admin',
      };

      const result = service.validateEventData(invalidEvent);
      expect(result).toBe(false);
    });

    test('Debe rechazar evento sin new_status', () => {
      const invalidEvent = {
        id_complaint: 123,
        changed_by: 'admin',
      };

      const result = service.validateEventData(invalidEvent);
      expect(result).toBe(false);
    });

    test('Debe rechazar evento con new_status inválido', () => {
      const invalidEvent = {
        id_complaint: 123,
        new_status: 'estado_invalido',
      };

      const result = service.validateEventData(invalidEvent);
      expect(result).toBe(false);
    });

    test('Debe rechazar evento con previous_status inválido', () => {
      const invalidEvent = {
        id_complaint: 123,
        new_status: 'cerrada',
        previous_status: 'estado_invalido',
      };

      const result = service.validateEventData(invalidEvent);
      expect(result).toBe(false);
    });

    test('Debe aceptar evento sin previous_status (creación inicial)', () => {
      const validEvent = {
        id_complaint: 123,
        new_status: 'abierta',
      };

      const result = service.validateEventData(validEvent);
      expect(result).toBe(true);
    });

    test('Debe validar los tres estados permitidos: abierta, en_revision, cerrada', () => {
      const statuses = ['abierta', 'en_revision', 'cerrada'];

      statuses.forEach((status) => {
        const event = {
          id_complaint: 123,
          new_status: status,
        };
        expect(service.validateEventData(event)).toBe(true);
      });
    });
  });

  // ==================== TEST 2: Guardado en base de datos ====================
  describe('Test 2: saveEventToDatabase - Persistencia de eventos', () => {
    test('Debe guardar evento completo en la base de datos correctamente', async () => {
      const eventData = {
        id_complaint: 456,
        previous_status: 'abierta',
        new_status: 'en_revision',
        changed_by: 'user@example.com',
        change_description: 'Iniciando revisión',
        event_timestamp: '2024-11-14T10:30:00.000Z',
      };

      await service.saveEventToDatabase(eventData);

      expect(ComplaintStatusHistory.create).toHaveBeenCalledTimes(1);
      expect(ComplaintStatusHistory.create).toHaveBeenCalledWith({
        id_complaint: 456,
        previous_status: 'abierta',
        new_status: 'en_revision',
        changed_by: 'user@example.com',
        change_description: 'Iniciando revisión',
        event_timestamp: new Date('2024-11-14T10:30:00.000Z'),
        created_at: expect.any(Date),
      });
    });

    test('Debe usar valores por defecto cuando faltan datos opcionales', async () => {
      const minimalEvent = {
        id_complaint: 789,
        new_status: 'abierta',
      };

      await service.saveEventToDatabase(minimalEvent);

      expect(ComplaintStatusHistory.create).toHaveBeenCalledWith({
        id_complaint: 789,
        previous_status: null,
        new_status: 'abierta',
        changed_by: 'system',
        change_description: null,
        event_timestamp: expect.any(Date),
        created_at: expect.any(Date),
      });
    });

    test('Debe usar "system" como changed_by por defecto', async () => {
      const eventWithoutUser = {
        id_complaint: 111,
        new_status: 'cerrada',
      };

      await service.saveEventToDatabase(eventWithoutUser);

      const callArgs = ComplaintStatusHistory.create.mock.calls[0][0];
      expect(callArgs.changed_by).toBe('system');
    });

    test('Debe manejar error de base de datos y propagar excepción', async () => {
      const dbError = new Error('Database connection failed');
      ComplaintStatusHistory.create.mockRejectedValue(dbError);

      const eventData = {
        id_complaint: 999,
        new_status: 'abierta',
      };

      await expect(service.saveEventToDatabase(eventData)).rejects.toThrow(
        'Database connection failed'
      );
    });
  });

  // ==================== TEST 3: Procesamiento de mensajes de Kafka ====================
  describe('Test 3: handleMessage - Procesamiento de eventos de Kafka', () => {
    test('Debe procesar mensaje válido correctamente', async () => {
      const validMessage = {
        value: Buffer.from(
          JSON.stringify({
            id_complaint: 123,
            previous_status: 'abierta',
            new_status: 'en_revision',
            changed_by: 'admin',
          })
        ),
        headers: {
          'event-type': Buffer.from('status-change'),
        },
      };

      await service.handleMessage(validMessage, 0);

      expect(ComplaintStatusHistory.create).toHaveBeenCalledTimes(1);
    });

    test('Debe rechazar mensaje con JSON inválido sin lanzar excepción', async () => {
      const invalidMessage = {
        value: Buffer.from('invalid-json{'),
        headers: {},
      };

      // No debe lanzar excepción para no bloquear el consumer
      await expect(
        service.handleMessage(invalidMessage, 0)
      ).resolves.not.toThrow();
      expect(ComplaintStatusHistory.create).not.toHaveBeenCalled();
    });

    test('Debe rechazar mensaje con datos inválidos sin lanzar excepción', async () => {
      const messageWithInvalidData = {
        value: Buffer.from(
          JSON.stringify({
            id_complaint: 123,
            new_status: 'estado_invalido',
          })
        ),
        headers: {},
      };

      await expect(
        service.handleMessage(messageWithInvalidData, 0)
      ).resolves.not.toThrow();
      expect(ComplaintStatusHistory.create).not.toHaveBeenCalled();
    });

    test('Debe procesar mensaje sin headers', async () => {
      const messageWithoutHeaders = {
        value: Buffer.from(
          JSON.stringify({
            id_complaint: 555,
            new_status: 'cerrada',
          })
        ),
      };

      await service.handleMessage(messageWithoutHeaders, 0);

      expect(ComplaintStatusHistory.create).toHaveBeenCalledTimes(1);
    });

    test('Debe continuar procesamiento aunque falle el guardado en BD', async () => {
      ComplaintStatusHistory.create.mockRejectedValue(
        new Error('DB Error')
      );

      const validMessage = {
        value: Buffer.from(
          JSON.stringify({
            id_complaint: 123,
            new_status: 'abierta',
          })
        ),
        headers: {},
      };

      // No debe lanzar excepción para no bloquear el consumer
      await expect(
        service.handleMessage(validMessage, 0)
      ).resolves.not.toThrow();
    });
  });

  // ==================== TEST 4: Inicialización y conexión ====================
  describe('Test 4: initialize - Inicialización del consumer', () => {
    test('Debe inicializar consumer correctamente cuando Kafka está habilitado', async () => {
      await service.initialize();

      expect(service.isConsumerConnected()).toBe(true);
      expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'complaint-status-events',
        fromBeginning: true,
      });
    });

    test('No debe inicializar consumer cuando Kafka está deshabilitado', async () => {
      kafkaConfig.enabled = false;

      await service.initialize();

      expect(service.isConsumerConnected()).toBe(false);
      expect(mockConsumer.connect).not.toHaveBeenCalled();
    });

    test('Debe manejar error de conexión a Kafka', async () => {
      mockConsumer.connect.mockRejectedValue(
        new Error('Connection refused')
      );

      await expect(service.initialize()).rejects.toThrow(
        'Connection refused'
      );
      expect(service.isConsumerConnected()).toBe(false);
    });

    test('Debe configurar Kafka con parámetros de retry correctos', async () => {
      const { Kafka } = require('kafkajs');

      await service.initialize();

      expect(Kafka).toHaveBeenCalledWith({
        clientId: 'test-client',
        brokers: ['localhost:9092'],
        connectionTimeout: 3000,
        requestTimeout: 3000,
        retry: {
          initialRetryTime: 100,
          retries: 5,
          maxRetryTime: 30000,
          multiplier: 2,
        },
      });
    });
  });

  // ==================== TEST 5: Ciclo de vida del consumer ====================
  describe('Test 5: Consumer lifecycle - Inicio y desconexión', () => {
    test('Debe iniciar consumo correctamente después de inicialización', async () => {
      await service.initialize();
      await service.startConsuming();

      expect(mockConsumer.run).toHaveBeenCalledTimes(1);
      expect(mockConsumer.run).toHaveBeenCalledWith({
        autoCommit: true,
        autoCommitInterval: 5000,
        eachMessage: expect.any(Function),
      });
    });

    test('Debe lanzar error al iniciar consumo sin inicialización previa', async () => {
      await expect(service.startConsuming()).rejects.toThrow(
        'Consumer not connected. Call initialize() first.'
      );
    });

    test('Debe desconectar consumer correctamente', async () => {
      await service.initialize();
      await service.disconnect();

      expect(mockConsumer.disconnect).toHaveBeenCalledTimes(1);
      expect(service.isConsumerConnected()).toBe(false);
    });

    test('No debe fallar al desconectar consumer no inicializado', async () => {
      await expect(service.disconnect()).resolves.not.toThrow();
      expect(mockConsumer.disconnect).not.toHaveBeenCalled();
    });

    test('Debe manejar error en desconexión', async () => {
      await service.initialize();
      mockConsumer.disconnect.mockRejectedValue(
        new Error('Disconnect failed')
      );

      await expect(service.disconnect()).rejects.toThrow(
        'Disconnect failed'
      );
    });
  });

  // ==================== TEST 6: Transiciones de estado válidas ====================
  describe('Test 6: Business Rules - Validación de transiciones de estado', () => {
    test('Debe permitir transición de abierta a en_revision', async () => {
      const event = {
        id_complaint: 100,
        previous_status: 'abierta',
        new_status: 'en_revision',
      };

      const isValid = service.validateEventData(event);
      expect(isValid).toBe(true);

      await service.saveEventToDatabase(event);
      expect(ComplaintStatusHistory.create).toHaveBeenCalled();
    });

    test('Debe permitir transición de en_revision a cerrada', async () => {
      const event = {
        id_complaint: 101,
        previous_status: 'en_revision',
        new_status: 'cerrada',
      };

      const isValid = service.validateEventData(event);
      expect(isValid).toBe(true);

      await service.saveEventToDatabase(event);
      expect(ComplaintStatusHistory.create).toHaveBeenCalled();
    });

    test('Debe permitir transición de abierta a cerrada (cierre directo)', async () => {
      const event = {
        id_complaint: 102,
        previous_status: 'abierta',
        new_status: 'cerrada',
      };

      const isValid = service.validateEventData(event);
      expect(isValid).toBe(true);

      await service.saveEventToDatabase(event);
      expect(ComplaintStatusHistory.create).toHaveBeenCalled();
    });

    test('Debe permitir reapertura: de cerrada a abierta', async () => {
      const event = {
        id_complaint: 103,
        previous_status: 'cerrada',
        new_status: 'abierta',
      };

      const isValid = service.validateEventData(event);
      expect(isValid).toBe(true);

      await service.saveEventToDatabase(event);
      expect(ComplaintStatusHistory.create).toHaveBeenCalled();
    });

    test('Debe permitir creación inicial sin previous_status', async () => {
      const event = {
        id_complaint: 104,
        new_status: 'abierta',
      };

      const isValid = service.validateEventData(event);
      expect(isValid).toBe(true);

      await service.saveEventToDatabase(event);
      const callArgs = ComplaintStatusHistory.create.mock.calls[0][0];
      expect(callArgs.previous_status).toBeNull();
    });
  });

  // ==================== TEST 7: Event Sourcing - Características ====================
  describe('Test 7: Event Sourcing - Características y comportamiento', () => {
    test('Debe preservar timestamp del evento original', async () => {
      const originalTimestamp = '2024-11-14T15:30:00.000Z';
      const event = {
        id_complaint: 200,
        new_status: 'abierta',
        event_timestamp: originalTimestamp,
      };

      await service.saveEventToDatabase(event);

      const callArgs = ComplaintStatusHistory.create.mock.calls[0][0];
      expect(callArgs.event_timestamp).toEqual(
        new Date(originalTimestamp)
      );
    });

    test('Debe usar timestamp actual si no se proporciona', async () => {
      const beforeTime = new Date();
      const event = {
        id_complaint: 201,
        new_status: 'abierta',
      };

      await service.saveEventToDatabase(event);

      const callArgs = ComplaintStatusHistory.create.mock.calls[0][0];
      const afterTime = new Date();

      expect(callArgs.event_timestamp.getTime()).toBeGreaterThanOrEqual(
        beforeTime.getTime()
      );
      expect(callArgs.event_timestamp.getTime()).toBeLessThanOrEqual(
        afterTime.getTime()
      );
    });

    test('Debe registrar información de auditoría: quién cambió el estado', async () => {
      const event = {
        id_complaint: 202,
        new_status: 'en_revision',
        changed_by: 'admin@boyaca.gov.co',
        change_description: 'Revisión de documentos',
      };

      await service.saveEventToDatabase(event);

      const callArgs = ComplaintStatusHistory.create.mock.calls[0][0];
      expect(callArgs.changed_by).toBe('admin@boyaca.gov.co');
      expect(callArgs.change_description).toBe('Revisión de documentos');
    });

    test('Debe procesar múltiples eventos para la misma queja (historial)', async () => {
      const events = [
        { id_complaint: 300, new_status: 'abierta' },
        {
          id_complaint: 300,
          previous_status: 'abierta',
          new_status: 'en_revision',
        },
        {
          id_complaint: 300,
          previous_status: 'en_revision',
          new_status: 'cerrada',
        },
      ];

      for (const event of events) {
        await service.saveEventToDatabase(event);
      }

      expect(ComplaintStatusHistory.create).toHaveBeenCalledTimes(3);
    });

    test('Debe poder reconstruir historial desde fromBeginning=true', async () => {
      await service.initialize();

      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'complaint-status-events',
        fromBeginning: true,
      });
    });
  });
});
