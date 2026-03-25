/**
 * Analytics Controller
 * ====================
 * Handles HTTP requests for analytics endpoints
 */

const analyticsService = require('../services/analyticsService');

class AnalyticsController {
  async getSessionsByTier(req, res) {
    try {
      const { daysBack = 90 } = req.query;
      const startTime = Date.now();
      
      const results = await analyticsService.getSessionsByTier(parseInt(daysBack));
      const executionTime = Date.now() - startTime;
      
      res.json({
        success: true,
        data: results,
        metadata: {
          executionTime: `${executionTime}ms`,
          timestamp: new Date().toISOString(),
          rowCount: results.length
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  async getFeatureDauRetention(req, res) {
    try {
      const { daysBack = 30, limit = 50 } = req.query;
      const startTime = Date.now();
      
      const results = await analyticsService.getFeatureDauRetention(
        parseInt(daysBack),
        parseInt(limit)
      );
      const executionTime = Date.now() - startTime;
      
      res.json({
        success: true,
        data: results,
        metadata: {
          executionTime: `${executionTime}ms`,
          timestamp: new Date().toISOString(),
          rowCount: results.length
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  async getOnboardingFunnel(req, res) {
    try {
      const startTime = Date.now();
      
      const results = await analyticsService.getOnboardingFunnel();
      const executionTime = Date.now() - startTime;
      
      res.json({
        success: true,
        data: results,
        metadata: {
          executionTime: `${executionTime}ms`,
          timestamp: new Date().toISOString(),
          rowCount: results.length
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  async getEngagedFreeUsers(req, res) {
    try {
      const { limit = 20, daysBack = 90 } = req.query;
      const startTime = Date.now();
      
      const results = await analyticsService.getEngagedFreeUsers(
        parseInt(limit),
        parseInt(daysBack)
      );
      const executionTime = Date.now() - startTime;
      
      res.json({
        success: true,
        data: results,
        metadata: {
          executionTime: `${executionTime}ms`,
          timestamp: new Date().toISOString(),
          rowCount: results.length
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  }

  async healthCheck(req, res) {
    res.json({
      status: 'ok',
      service: 'RoaDo Analytics API',
      timestamp: new Date().toISOString()
    });
  }
}

module.exports = new AnalyticsController();
