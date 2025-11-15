/**
 * Database Configuration using Sequelize
 * Connects to MySQL database with historical schema
 */

require('dotenv').config();
const { Sequelize } = require('sequelize');

const sequelize = new Sequelize(
  process.env.DB_NAME || 'complaints_db',
  process.env.DB_USER || 'root',
  process.env.DB_PASSWORD || 'root',
  {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    dialect: process.env.DB_DIALECT || 'mysql',
    logging: process.env.DB_LOGGING === 'true' ? console.log : false,
    pool: {
      max: 5,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
    timezone: '-05:00', // Colombia timezone
    define: {
      timestamps: false,
      freezeTableName: true,
    },
  }
);

/**
 * Test database connection
 * @returns {Promise<void>}
 */
async function testConnection() {
  try {
    await sequelize.authenticate();
    console.log('[OK] Database connection established successfully');
  } catch (error) {
    console.error('[ERROR] Unable to connect to database:', error.message);
    throw error;
  }
}

module.exports = { sequelize, testConnection };
