/**
 * PostgreSQL Connection Module
 * =============================
 * Manages database connection to nimbus_core PostgreSQL database
 * Provides connection pooling and query execution utilities
 */

const { Pool } = require('pg');
const path = require('path');
require('dotenv').config();

class PostgreSQLConnection {
  constructor() {
    this.pool = null;
  }

  async connect() {
    try {
      const config = {
        host: process.env.POSTGRES_HOST || 'localhost',
        port: process.env.POSTGRES_PORT || 5432,
        database: process.env.POSTGRES_DB || 'nimbus_core',
        user: process.env.POSTGRES_USER || 'postgres',
        password: process.env.POSTGRES_PASSWORD || 'postgres',
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
      };

      this.pool = new Pool(config);

      // Test connection
      const result = await this.pool.query('SELECT NOW()');
      console.log('✓ PostgreSQL connected successfully at', result.rows[0].now);
      
      return true;
    } catch (error) {
      console.error('✗ PostgreSQL connection failed:', error.message);
      throw error;
    }
  }

  async close() {
    if (this.pool) {
      await this.pool.end();
      console.log('PostgreSQL connection pool closed');
    }
  }

  async query(text, params = []) {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call connect() first.');
    }
    
    try {
      const result = await this.pool.query(text, params);
      return result.rows;
    } catch (error) {
      console.error('Query error:', error);
      throw error;
    }
  }

  async executeBatch(queries) {
    if (!this.pool) {
      throw new Error('Database pool not initialized. Call connect() first.');
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      for (const query of queries) {
        await client.query(query);
      }
      
      await client.query('COMMIT');
      console.log(`✓ Batch of ${queries.length} queries executed successfully`);
    } catch (error) {
      await client.query('ROLLBACK');
      console.error('Batch execution error:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  getPool() {
    return this.pool;
  }
}

module.exports = new PostgreSQLConnection();
