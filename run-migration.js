require('dotenv').config();
const { sequelize } = require('./src/config/db');

async function runMigration() {
  try {
    console.log('✓ Conectado a la base de datos');

    // Agregar columna correlation_id
    await sequelize.query(`
      ALTER TABLE \`historical.complaint_status_history\` 
      ADD COLUMN correlation_id VARCHAR(255) NULL
    `);
    console.log('✓ Columna correlation_id agregada');

    // Crear índice
    await sequelize.query(`
      CREATE INDEX idx_correlation_id 
      ON \`historical.complaint_status_history\`(correlation_id)
    `);
    console.log('✓ Índice idx_correlation_id creado');

    // Verificar
    const [results] = await sequelize.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = 'dbcomplaints'
      AND table_name = 'complaint_status_history'
      AND column_name = 'correlation_id'
    `);

    if (results.length > 0) {
      console.log('✓ Migración completada exitosamente');
      console.log('  Columna:', results[0]);
    }

    await sequelize.close();
    process.exit(0);
  } catch (error) {
    console.error('✗ Error en migración:', error.message);
    await sequelize.close();
    process.exit(1);
  }
}

runMigration();
