const logViewerService = require('../services/logViewerService');
const path = require('path');

class LogsController {
  /**
   * Renderizar la interfaz de visualización de logs
   * GET /logs
   */
  async renderLogsViewer(req, res) {
    try {
      res.sendFile(path.join(__dirname, '../views/logs-viewer.html'));
    } catch (error) {
      console.error('Error rendering logs viewer:', error);
      res.status(500).send('Error al cargar el visor de logs');
    }
  }

  /**
   * Get list of available log files
   * GET /api/logs/files
   */
  async getLogFiles(req, res) {
    try {
      const files = await logViewerService.getLogFiles();
      res.json({
        success: true,
        data: files
      });
    } catch (error) {
      console.error('Error getting log files:', error);
      res.status(500).json({
        success: false,
        message: 'Error al obtener archivos de logs',
        error: error.message
      });
    }
  }

  /**
   * Get logs from a specific file with optional filters
   * GET /api/logs/:filename
   * Query params: level, correlationId, search, limit, offset, reverse
   */
  async getLogs(req, res) {
    try {
      const { filename } = req.params;
      const {
        level,
        correlationId,
        search,
        limit = 500,
        offset = 0,
        reverse = false
      } = req.query;

      const result = await logViewerService.readLogs(filename, {
        level,
        correlationId,
        search,
        limit: parseInt(limit),
        offset: parseInt(offset),
        reverse: reverse === 'true'
      });

      res.json({
        success: true,
        data: result
      });
    } catch (error) {
      console.error('Error reading logs:', error);
      res.status(500).json({
        success: false,
        message: 'Error al leer logs',
        error: error.message
      });
    }
  }

  /**
   * Search logs by correlation ID across all files
   * GET /api/logs/search/:correlationId
   */
  async searchByCorrelationId(req, res) {
    try {
      const { correlationId } = req.params;
      
      if (!correlationId) {
        return res.status(400).json({
          success: false,
          message: 'Correlation ID es requerido'
        });
      }

      const results = await logViewerService.searchByCorrelationId(correlationId);
      
      res.json({
        success: true,
        data: {
          correlationId,
          results,
          totalFiles: results.length,
          totalLogs: results.reduce((sum, r) => sum + r.logs.length, 0)
        }
      });
    } catch (error) {
      console.error('Error searching by correlation ID:', error);
      res.status(500).json({
        success: false,
        message: 'Error al buscar por correlation ID',
        error: error.message
      });
    }
  }

  /**
   * Get recent errors across all log files
   * GET /api/logs/errors/recent
   * Query params: limit
   */
  async getRecentErrors(req, res) {
    try {
      const limit = parseInt(req.query.limit) || 50;
      const errors = await logViewerService.getRecentErrors(limit);

      res.json({
        success: true,
        data: {
          errors,
          total: errors.length
        }
      });
    } catch (error) {
      console.error('Error getting recent errors:', error);
      res.status(500).json({
        success: false,
        message: 'Error al obtener errores recientes',
        error: error.message
      });
    }
  }

  /**
   * Get business logs (only complaint status change events)
   * GET /api/logs/business
   */
  async getBusinessLogs(req, res) {
    try {
      const { limit = 100 } = req.query;
      
      // Get only business-related logs (status change events)
      const logs = await logViewerService.getBusinessLogs(parseInt(limit));
      
      res.json({
        success: true,
        data: logs,
        count: logs.length
      });
    } catch (error) {
      console.error('Error getting business logs:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
}

module.exports = new LogsController();
