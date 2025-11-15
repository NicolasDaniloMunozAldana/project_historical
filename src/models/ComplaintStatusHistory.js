/**
 * ComplaintStatusHistory Model
 * Stores event sourcing data for complaint status changes
 * Schema: historical
 */

const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/db');

const ComplaintStatusHistory = sequelize.define(
  'ComplaintStatusHistory',
  {
    id_history: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
      allowNull: false,
    },
    id_complaint: {
      type: DataTypes.INTEGER,
      allowNull: false,
      comment: 'ID de la queja relacionada',
    },
    previous_status: {
      type: DataTypes.ENUM('abierta', 'en_revision', 'cerrada'),
      allowNull: true,
      comment: 'Estado anterior de la queja',
    },
    new_status: {
      type: DataTypes.ENUM('abierta', 'en_revision', 'cerrada'),
      allowNull: false,
      comment: 'Nuevo estado de la queja',
    },
    changed_by: {
      type: DataTypes.STRING(100),
      allowNull: true,
      comment: 'Usuario que realizó el cambio',
    },
    change_description: {
      type: DataTypes.TEXT,
      allowNull: true,
      comment: 'Descripción del cambio',
    },
    event_timestamp: {
      type: DataTypes.DATE,
      allowNull: false,
      comment: 'Fecha y hora del evento',
    },
    created_at: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
      field: 'created_at',
    },
  },
  {
    schema: 'historical',
    tableName: 'complaint_status_history',
    timestamps: false,
    indexes: [
      {
        name: 'idx_complaint_id',
        fields: ['id_complaint'],
      },
      {
        name: 'idx_event_timestamp',
        fields: ['event_timestamp'],
      },
      {
        name: 'idx_new_status',
        fields: ['new_status'],
      },
    ],
  }
);

module.exports = ComplaintStatusHistory;
