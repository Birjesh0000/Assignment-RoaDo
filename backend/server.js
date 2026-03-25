/**
 * Express.js Backend Server
 * =========================
 * RoaDo Analytics API - serves dashboard queries
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const db = require('./src/config/database');
const analyticsRoutes = require('./src/routes/analytics');

const app = express();
const PORT = process.env.PORT || 5000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/nimbus_events';
const DB_NAME = 'nimbus_events';

// Middleware
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());

// Error handler middleware
app.use((error, req, res, next) => {
  console.error('Error:', error);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: error.message
  });
});

// Routes
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'RoaDo Analytics Backend',
    version: '1.0.0',
    timestamp: new Date().toISOString()
  });
});

// Analytics API routes
app.use('/api/v1/analytics', analyticsRoutes);

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Not found',
    path: req.path
  });
});

// Start server
async function start() {
  try {
    console.log('\n=== RoaDo Analytics Backend ===\n');
    
    // Connect to MongoDB
    await db.connect(MONGODB_URI, DB_NAME);
    
    // Start Express server
    app.listen(PORT, () => {
      console.log(`✓ Server running on http://localhost:${PORT}`);
      console.log(`✓ API endpoints available at http://localhost:${PORT}/api/v1/analytics`);
      console.log(`✓ Health check at http://localhost:${PORT}/api/health\n`);
    });
  } catch (error) {
    console.error('✗ Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down gracefully...');
  await db.close();
  process.exit(0);
});

start();

module.exports = app;
