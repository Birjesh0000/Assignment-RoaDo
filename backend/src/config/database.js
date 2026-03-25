/**
 * MongoDB Connection Module
 * =========================
 * Manages database connection and provides collection access
 */

const { MongoClient } = require('mongodb');

class DatabaseConnection {
  constructor() {
    this.client = null;
    this.db = null;
  }

  async connect(mongoUri, dbName) {
    try {
      this.client = new MongoClient(mongoUri, {
        serverSelectionTimeoutMS: 5000,
        connectTimeoutMS: 10000,
      });

      await this.client.connect();
      this.db = this.client.db(dbName);
      
      // Verify connection
      await this.db.admin().ping();
      console.log('✓ MongoDB connected successfully');
      
      return true;
    } catch (error) {
      console.error('✗ MongoDB connection failed:', error.message);
      throw error;
    }
  }

  async close() {
    if (this.client) {
      await this.client.close();
      console.log('MongoDB connection closed');
    }
  }

  getCollection(collectionName) {
    if (!this.db) {
      throw new Error('Database not connected');
    }
    return this.db.collection(collectionName);
  }

  getDatabase() {
    return this.db;
  }
}

module.exports = new DatabaseConnection();
