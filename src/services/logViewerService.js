const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
const { createReadStream } = require('fs');

class LogViewerService {
  constructor() {
    this.logsDir = path.join(__dirname, '../../logs');
  }

  /**
   * Get list of available log files
   */
  async getLogFiles() {
    try {
      const files = await fs.readdir(this.logsDir);
      const logFiles = files.filter(file => file.endsWith('.log'));
      
      const filesWithStats = await Promise.all(
        logFiles.map(async (file) => {
          const filePath = path.join(this.logsDir, file);
          const stats = await fs.stat(filePath);
          return {
            name: file,
            size: stats.size,
            modified: stats.mtime,
            path: filePath
          };
        })
      );

      // Sort by modified date, most recent first
      return filesWithStats.sort((a, b) => b.modified - a.modified);
    } catch (error) {
      console.error('Error reading log files:', error);
      return [];
    }
  }

  /**
   * Read and parse log entries from a file
   * @param {string} filename - Name of the log file
   * @param {object} filters - Filters to apply (level, correlationId, search, limit, reverse)
   */
  async readLogs(filename, filters = {}) {
    const {
      level = null,
      correlationId = null,
      search = null,
      limit = 500,
      offset = 0,
      reverse = false // Si es true, lee desde el final (logs más recientes primero)
    } = filters;

    const filePath = path.join(this.logsDir, filename);
    let allLogs = [];
    
    try {
      const fileStream = createReadStream(filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let lineNumber = 0;

      // Primero, leer todas las líneas que coincidan con los filtros
      for await (const line of rl) {
        lineNumber++;
        
        if (!line.trim()) continue;

        try {
          const logEntry = JSON.parse(line);
          
          // Apply filters
          if (level && logEntry.level !== level) continue;
          if (correlationId && logEntry.correlationId !== correlationId) continue;
          if (search) {
            const searchLower = search.toLowerCase();
            const matchesSearch = 
              (logEntry.message && logEntry.message.toLowerCase().includes(searchLower)) ||
              (logEntry.service && logEntry.service.toLowerCase().includes(searchLower)) ||
              (logEntry.operation && logEntry.operation && logEntry.operation.toLowerCase().includes(searchLower)) ||
              (logEntry.error && JSON.stringify(logEntry.error).toLowerCase().includes(searchLower));
            
            if (!matchesSearch) continue;
          }

          allLogs.push({
            ...logEntry,
            lineNumber
          });
        } catch {
          // Skip invalid JSON lines
          continue;
        }
      }

      // Si reverse es true, invertir el orden (logs más recientes primero)
      if (reverse) {
        allLogs.reverse();
      }

      // Ordenar por timestamp si está disponible (más recientes primero)
      allLogs.sort((a, b) => {
        const timeA = a.timestamp ? new Date(a.timestamp).getTime() : 0;
        const timeB = b.timestamp ? new Date(b.timestamp).getTime() : 0;
        return timeB - timeA; // Descendente (más reciente primero)
      });

      const totalMatched = allLogs.length;

      // Apply offset
      const logs = allLogs.slice(offset, offset + limit);

      return {
        logs,
        totalMatched,
        hasMore: totalMatched > (offset + limit)
      };
    } catch (error) {
      console.error('Error reading log file:', error);
      throw error;
    }
  }

  /**
   * Get log statistics for a file
   */
  async getLogStats(filename) {
    const filePath = path.join(this.logsDir, filename);
    const stats = {
      totalLines: 0,
      byLevel: {
        error: 0,
        warn: 0,
        info: 0,
        debug: 0
      },
      byService: {},
      correlationIds: new Set(),
      timeRange: {
        first: null,
        last: null
      }
    };

    try {
      const fileStream = createReadStream(filePath);
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (!line.trim()) continue;

        try {
          const logEntry = JSON.parse(line);
          stats.totalLines++;

          // Count by level
          if (logEntry.level && stats.byLevel[logEntry.level] !== undefined) {
            stats.byLevel[logEntry.level]++;
          }

          // Count by service
          if (logEntry.service) {
            stats.byService[logEntry.service] = (stats.byService[logEntry.service] || 0) + 1;
          }

          // Track correlation IDs
          if (logEntry.correlationId) {
            stats.correlationIds.add(logEntry.correlationId);
          }

          // Track time range
          if (logEntry.timestamp) {
            const timestamp = new Date(logEntry.timestamp);
            if (!stats.timeRange.first || timestamp < stats.timeRange.first) {
              stats.timeRange.first = timestamp;
            }
            if (!stats.timeRange.last || timestamp > stats.timeRange.last) {
              stats.timeRange.last = timestamp;
            }
          }
        } catch {
          // Skip invalid JSON lines
          continue;
        }
      }

      stats.uniqueCorrelationIds = stats.correlationIds.size;
      delete stats.correlationIds; // Don't send all IDs to client

      return stats;
    } catch (error) {
      console.error('Error calculating log stats:', error);
      throw error;
    }
  }

  /**
   * Search logs across all files for a correlation ID
   */
  async searchByCorrelationId(correlationId) {
    const files = await this.getLogFiles();
    const results = [];

    for (const file of files) {
      const { logs } = await this.readLogs(file.name, {
        correlationId,
        limit: 1000
      });

      if (logs.length > 0) {
        results.push({
          file: file.name,
          logs
        });
      }
    }

    return results;
  }

  /**
   * Get recent errors across all log files
   */
  async getRecentErrors(limit = 50) {
    const files = await this.getLogFiles();
    const errors = [];

    for (const file of files) {
      const { logs } = await this.readLogs(file.name, {
        level: 'error',
        limit: limit
      });

      errors.push(...logs.map(log => ({
        ...log,
        file: file.name
      })));

      if (errors.length >= limit) {
        break;
      }
    }

    // Sort by timestamp, most recent first
    return errors
      .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
      .slice(0, limit);
  }

  /**
   * Get business logs - only complaint status change events
   * @param {number} limit - Maximum number of logs to return
   */
  async getBusinessLogs(limit = 100) {
    try {
      const files = await this.getLogFiles();
      const businessLogs = [];

      // Business event types we care about
      const businessEvents = [
        'EVENTO_CONSUMIDO_KAFKA',
        'EVENTO_GUARDADO_EXITOSO',
        'EVENT_VALIDATION_ERROR',
        'DATABASE_ERROR'
      ];

      for (const file of files) {
        const filePath = path.join(this.logsDir, file.name);
        const fileStream = createReadStream(filePath);
        const rl = readline.createInterface({
          input: fileStream,
          crlfDelay: Infinity
        });

        for await (const line of rl) {
          if (!line.trim()) continue;

          try {
            const logEntry = JSON.parse(line);
            
            // Filter only business events
            if (logEntry.eventType && businessEvents.includes(logEntry.eventType)) {
              businessLogs.push({
                timestamp: logEntry.timestamp,
                level: logEntry.level,
                eventType: logEntry.eventType,
                message: logEntry.message,
                correlationId: logEntry.correlationId,
                service: logEntry.service,
                // Los datos pueden estar en metadata, eventData o businessEvent.eventData
                complaintId: logEntry.eventData?.complaintId || logEntry.metadata?.complaintId,
                previousStatus: logEntry.eventData?.previousStatus || logEntry.metadata?.previousStatus,
                newStatus: logEntry.eventData?.newStatus || logEntry.metadata?.newStatus,
                historyId: logEntry.eventData?.historyId || logEntry.metadata?.historyId,
                processingTime: logEntry.eventData?.processingTimeMs || logEntry.metadata?.processingTimeMs,
                error: logEntry.error
              });
            }
          } catch (parseError) {
            // Skip invalid JSON lines
            continue;
          }

          if (businessLogs.length >= limit * 2) break;
        }

        if (businessLogs.length >= limit * 2) break;
      }

      // Sort by timestamp, most recent first
      return businessLogs
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, limit);
    } catch (error) {
      console.error('Error getting business logs:', error);
      return [];
    }
  }
}

module.exports = new LogViewerService();
