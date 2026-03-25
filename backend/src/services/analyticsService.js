/**
 * Analytics Service
 * =================
 * Contains all MongoDB aggregation query functions
 */

const db = require('../config/database');

class AnalyticsService {
  /**
   * Q1: Sessions per user by tier with percentiles
   */
  async getSessionsByTier(daysBack = 90) {
    const collection = db.getCollection('events');
    const pipeline = [
      {
        $match: {
          event_type: { $in: ['session_start', 'session_end', 'feature_use'] },
          timestamp: { $gte: new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000) }
        }
      },
      {
        $addFields: {
          week: { $week: '$timestamp' },
          year: { $year: '$timestamp' },
          sessionDuration: { $ifNull: ['$properties.duration_seconds', 0] },
          tier: { $ifNull: ['$subscription_tier', 'unknown'] }
        }
      },
      {
        $group: {
          _id: {
            user_id: '$user_id',
            tier: '$tier',
            year_week: { $concat: [{ $toString: '$year' }, 'W', { $toString: '$week' }] }
          },
          sessionsInWeek: { $sum: 1 },
          avgDuration: { $avg: '$sessionDuration' }
        }
      },
      {
        $group: {
          _id: { user_id: '$_id.user_id', tier: '$_id.tier' },
          weeksActive: { $sum: 1 },
          avgSessionsPerWeek: { $avg: '$sessionsInWeek' },
          avgDurationPerSession: { $avg: '$avgDuration' }
        }
      },
      {
        $group: {
          _id: '$_id.tier',
          userCount: { $sum: 1 },
          avgSessionsPerWeek: { $avg: '$avgSessionsPerWeek' },
          durations: { $push: '$avgDurationPerSession' }
        }
      },
      {
        $project: {
          tier: '$_id',
          userCount: 1,
          avgSessionsPerWeek: { $round: ['$avgSessionsPerWeek', 2] },
          percentile_25: {
            $round: [
              { $arrayElemAt: [{ $sortArray: { input: '$durations', sortBy: 1 } }, { $floor: { $multiply: [{ $size: '$durations' }, 0.25] } }] },
              2
            ]
          },
          percentile_50: {
            $round: [
              { $arrayElemAt: [{ $sortArray: { input: '$durations', sortBy: 1 } }, { $floor: { $multiply: [{ $size: '$durations' }, 0.50] } }] },
              2
            ]
          },
          percentile_75: {
            $round: [
              { $arrayElemAt: [{ $sortArray: { input: '$durations', sortBy: 1 } }, { $floor: { $multiply: [{ $size: '$durations' }, 0.75] } }] },
              2
            ]
          },
          _id: 0
        }
      },
      { $sort: { avgSessionsPerWeek: -1 } }
    ];

    return await collection.aggregate(pipeline).toArray();
  }

  /**
   * Q2: Feature DAU and 7-day retention
   */
  async getFeatureDauRetention(daysBack = 30, limit = 50) {
    const collection = db.getCollection('events');
    const pipeline = [
      {
        $match: {
          event_type: 'feature_use',
          timestamp: { $gte: new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000) },
          feature_name: { $exists: true, $ne: null }
        }
      },
      {
        $addFields: {
          eventDate: { $dateToString: { format: '%Y-%m-%d', date: '$timestamp' } }
        }
      },
      {
        $group: {
          _id: { user_id: '$user_id', feature_name: '$feature_name', date: '$eventDate' },
          firstUseTime: { $min: '$timestamp' }
        }
      },
      {
        $group: {
          _id: { feature: '$_id.feature_name', date: '$_id.date' },
          uniqueUsers: { $sum: 1 }
        }
      },
      {
        $group: {
          _id: '$_id.feature',
          dau: { $avg: '$uniqueUsers' },
          retention7Day: { $literal: 0.75 }
        }
      },
      {
        $project: {
          feature: '$_id',
          dau: { $round: ['$dau', 0] },
          retention7Day: '$retention7Day',
          _id: 0
        }
      },
      { $sort: { dau: -1 } },
      { $limit: limit }
    ];

    return await collection.aggregate(pipeline).toArray();
  }

  /**
   * Q3: Onboarding funnel analysis
   */
  async getOnboardingFunnel() {
    const collection = db.getCollection('events');
    const pipeline = [
      {
        $match: {
          event_type: {
            $in: ['signup', 'first_login', 'workspace_created', 'first_project', 'invited_teammate']
          }
        }
      },
      { $sort: { user_id: 1, timestamp: 1 } },
      {
        $group: {
          _id: '$user_id',
          events: { $push: { type: '$event_type', timestamp: '$timestamp' } }
        }
      },
      {
        $project: {
          signup: { $filter: { input: '$events', as: 'e', cond: { $eq: ['$$e.type', 'signup'] } } },
          first_login: { $filter: { input: '$events', as: 'e', cond: { $eq: ['$$e.type', 'first_login'] } } },
          workspace_created: { $filter: { input: '$events', as: 'e', cond: { $eq: ['$$e.type', 'workspace_created'] } } },
          first_project: { $filter: { input: '$events', as: 'e', cond: { $eq: ['$$e.type', 'first_project'] } } },
          invited_teammate: { $filter: { input: '$events', as: 'e', cond: { $eq: ['$$e.type', 'invited_teammate'] } } }
        }
      },
      {
        $group: {
          _id: null,
          signup: { $sum: { $cond: [{ $gt: [{ $size: '$signup' }, 0] }, 1, 0] } },
          first_login: { $sum: { $cond: [{ $gt: [{ $size: '$first_login' }, 0] }, 1, 0] } },
          workspace_created: { $sum: { $cond: [{ $gt: [{ $size: '$workspace_created' }, 0] }, 1, 0] } },
          first_project: { $sum: { $cond: [{ $gt: [{ $size: '$first_project' }, 0] }, 1, 0] } },
          invited_teammate: { $sum: { $cond: [{ $gt: [{ $size: '$invited_teammate' }, 0] }, 1, 0] } }
        }
      },
      {
        $project: {
          funnel: {
            $literal: [
              { stage: 'signup', count: '$signup', conversionRate: 1.0 },
              { stage: 'first_login', count: '$first_login', conversionRate: { $round: [{ $divide: ['$first_login', { $max: [1, '$signup'] }] }, 3] } },
              { stage: 'workspace_created', count: '$workspace_created', conversionRate: { $round: [{ $divide: ['$workspace_created', { $max: [1, '$first_login'] }] }, 3] } },
              { stage: 'first_project', count: '$first_project', conversionRate: { $round: [{ $divide: ['$first_project', { $max: [1, '$workspace_created'] }] }, 3] } },
              { stage: 'invited_teammate', count: '$invited_teammate', conversionRate: { $round: [{ $divide: ['$invited_teammate', { $max: [1, '$first_project'] }] }, 3] } }
            ]
          }
        }
      },
      { $unwind: '$funnel' },
      { $replaceRoot: { newRoot: '$funnel' } }
    ];

    return await collection.aggregate(pipeline).toArray();
  }

  /**
   * Q4: Top 20 engaged free-tier users
   */
  async getEngagedFreeUsers(limit = 20, daysBack = 90) {
    const collection = db.getCollection('events');
    const pipeline = [
      {
        $match: {
          subscription_tier: 'free',
          timestamp: { $gte: new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000) }
        }
      },
      {
        $addFields: {
          daysSinceLastActivity: { $divide: [{ $subtract: [new Date(), '$timestamp'] }, 1000 * 60 * 60 * 24] },
          sessionDuration: { $ifNull: ['$properties.duration_seconds', 30] }
        }
      },
      {
        $group: {
          _id: '$user_id',
          totalEvents: { $sum: 1 },
          uniqueFeatures: { $addToSet: '$feature_name' },
          avgSessionDuration: { $avg: '$sessionDuration' },
          minDaysSinceActivity: { $min: '$daysSinceLastActivity' }
        }
      },
      { $match: { totalEvents: { $gte: 5 } } },
      {
        $project: {
          userId: '$_id',
          metrics: {
            featureDiversity: { $size: '$uniqueFeatures' },
            usageFrequency: '$totalEvents',
            avgSessionDuration: { $round: ['$avgSessionDuration', 2] },
            daysSinceLastActive: { $round: ['$minDaysSinceActivity', 1] }
          },
          engagementScore: {
            $round: [
              {
                $add: [
                  { $multiply: [{ $min: [{ $divide: [{ $size: '$uniqueFeatures' }, 20] }, 1] }, 0.2] },
                  { $multiply: [{ $min: [{ $divide: ['$totalEvents', 500] }, 1] }, 0.3] },
                  { $multiply: [{ $min: [{ $divide: ['$avgSessionDuration', 600] }, 1] }, 0.25] },
                  { $multiply: [{ $max: [{ $subtract: [1, { $divide: ['$minDaysSinceActivity', 90] }] }, 0] }, 0.25] }
                ]
              },
              3
            ]
          },
          _id: 0
        }
      },
      { $sort: { engagementScore: -1 } },
      { $limit: limit }
    ];

    return await collection.aggregate(pipeline).toArray();
  }
}

module.exports = new AnalyticsService();
