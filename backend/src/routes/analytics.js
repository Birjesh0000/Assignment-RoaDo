/**
 * Analytics Routes
 * ================
 * API endpoint definitions for analytics queries
 */

const express = require('express');
const router = express.Router();
const analyticsController = require('../controllers/analyticsController');

/**
 * GET /api/v1/analytics/sessions-by-tier
 * Returns sessions per user by subscription tier with percentiles
 * Query params: daysBack (default: 90)
 */
router.get('/sessions-by-tier', analyticsController.getSessionsByTier);

/**
 * GET /api/v1/analytics/feature-dau-retention
 * Returns daily active users and 7-day retention per feature
 * Query params: daysBack (default: 30), limit (default: 50)
 */
router.get('/feature-dau-retention', analyticsController.getFeatureDauRetention);

/**
 * GET /api/v1/analytics/onboarding-funnel
 * Returns onboarding funnel with drop-off rates
 */
router.get('/onboarding-funnel', analyticsController.getOnboardingFunnel);

/**
 * GET /api/v1/analytics/engaged-free-users
 * Returns top engaged free-tier users for upsell targeting
 * Query params: limit (default: 20), daysBack (default: 90)
 */
router.get('/engaged-free-users', analyticsController.getEngagedFreeUsers);

/**
 * GET /api/v1/analytics/health
 * Health check endpoint
 */
router.get('/health', analyticsController.healthCheck);

module.exports = router;
