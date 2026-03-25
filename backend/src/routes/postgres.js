/**
 * PostgreSQL Analytics Routes
 * ===========================
 * REST API endpoints for accessing PostgreSQL analytics queries
 */

const express = require('express');
const postgresService = require('../services/postgresAnalyticsService');

const router = express.Router();

// Error handler middleware
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

/**
 * Q1: GET /api/v1/postgres/plan-metrics
 * Plan metrics with support analysis (last 6 months)
 */
router.get('/plan-metrics', asyncHandler(async (req, res) => {
  const result = await postgresService.getPlanMetricsWithSupport();
  res.json(result);
}));

/**
 * Q2: GET /api/v1/postgres/customer-ltv-ranking
 * Customer LTV ranking by tier with percentile analysis
 */
router.get('/customer-ltv-ranking', asyncHandler(async (req, res) => {
  const result = await postgresService.getCustomerLTVRankingByTier();
  res.json(result);
}));

/**
 * Q3: GET /api/v1/postgres/downgraded-customers
 * High-risk downgraded customers (last 90 days, 3+ support tickets)
 */
router.get('/downgraded-customers', asyncHandler(async (req, res) => {
  const result = await postgresService.getDowngradedCustomersHighRisk();
  res.json(result);
}));

/**
 * Q4: GET /api/v1/postgres/growth-churn-analysis
 * Month-over-month growth and churn rate analysis (last 24 months)
 */
router.get('/growth-churn-analysis', asyncHandler(async (req, res) => {
  const result = await postgresService.getMonthlyGrowthAndChurnAnalysis();
  res.json(result);
}));

/**
 * Q5: GET /api/v1/postgres/duplicate-customers
 * Potential duplicate customer detection with risk scoring
 */
router.get('/duplicate-customers', asyncHandler(async (req, res) => {
  const result = await postgresService.getDuplicateCustomerDetection();
  res.json(result);
}));

/**
 * Utility Endpoints
 */

/**
 * GET /api/v1/postgres/customers
 * Get all customers
 */
router.get('/customers', asyncHandler(async (req, res) => {
  const result = await postgresService.getAllCustomers();
  res.json(result);
}));

/**
 * GET /api/v1/postgres/subscription-stats
 * Get subscription statistics by tier
 */
router.get('/subscription-stats', asyncHandler(async (req, res) => {
  const result = await postgresService.getSubscriptionStatistics();
  res.json(result);
}));

/**
 * GET /api/v1/postgres/health
 * Health check for PostgreSQL connection
 */
router.get('/health', asyncHandler(async (req, res) => {
  try {
    const pool = require('../config/postgres').getPool();
    if (!pool) {
      return res.status(503).json({
        success: false,
        status: 'PostgreSQL not initialized'
      });
    }

    const result = await require('../config/postgres').query('SELECT NOW()');
    res.json({
      success: true,
      status: 'PostgreSQL connected',
      timestamp: result[0].now
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      status: 'PostgreSQL connection failed',
      error: error.message
    });
  }
}));

module.exports = router;
