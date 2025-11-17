-- Migration: Add correlation_id field to complaint_status_history table
-- This field enables end-to-end traceability across microservices

-- Add correlation_id column if it doesn't exist
ALTER TABLE historical.complaint_status_history 
ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(36) NULL 
COMMENT 'UUID para trazabilidad entre microservicios';

-- Add index for faster lookups by correlation_id
CREATE INDEX IF NOT EXISTS idx_correlation_id 
ON historical.complaint_status_history(correlation_id);

-- Verify the column was added
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'historical'
  AND TABLE_NAME = 'complaint_status_history'
  AND COLUMN_NAME = 'correlation_id';

