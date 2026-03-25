/**
 * API Service
 * ===========
 * Axios configuration and API calls
 */

import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000/api/v1';

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json'
  }
});

// Error interceptor
client.interceptors.response.use(
  response => response,
  error => {
    console.error('API Error:', error);
    throw error;
  }
);

export const analyticsApi = {
  // Get sessions by tier
  getSessionsByTier: (daysBack = 90) => 
    client.get('/analytics/sessions-by-tier', { params: { daysBack } }),
  
  // Get feature DAU and retention
  getFeatureDauRetention: (daysBack = 30, limit = 50) =>
    client.get('/analytics/feature-dau-retention', { params: { daysBack, limit } }),
  
  // Get onboarding funnel
  getOnboardingFunnel: () =>
    client.get('/analytics/onboarding-funnel'),
  
  // Get engaged free users
  getEngagedFreeUsers: (limit = 20, daysBack = 90) =>
    client.get('/analytics/engaged-free-users', { params: { limit, daysBack } }),
  
  // Health check
  health: () =>
    client.get('/analytics/health')
};

export default client;
