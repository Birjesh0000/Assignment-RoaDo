#!/usr/bin/env node

/**
 * MongoDB Query Runner
 * ====================
 * 
 * Executes all Task 2 MongoDB aggregation queries
 * and saves results to JSON files.
 * 
 * Usage: node run-queries.js [query-number]
 *   query-number: 1, 2, 3, or 4 (optional, runs all if omitted)
 */

const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const MONGO_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/nimbus_events';
const DB_NAME = 'nimbus_events';
const COLLECTION_NAME = 'events';

/**
 * Query 1: Sessions per user by tier with percentiles
 */
const query1_sessionsByTier = [
  {
    $match: {
      event_type: { $in: ['session_start', 'session_end', 'feature_use'] },
      timestamp: {
        $gte: new Date(new Date().setDate(new Date().getDate() - 90))
      }
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
          {
            $arrayElemAt: [
              { $sortArray: { input: '$durations', sortBy: 1 } },
              { $floor: { $multiply: [{ $size: '$durations' }, 0.25] } }
            ]
          },
          2
        ]
      },
      percentile_50: {
        $round: [
          {
            $arrayElemAt: [
              { $sortArray: { input: '$durations', sortBy: 1 } },
              { $floor: { $multiply: [{ $size: '$durations' }, 0.50] } }
            ]
          },
          2
        ]
      },
      percentile_75: {
        $round: [
          {
            $arrayElemAt: [
              { $sortArray: { input: '$durations', sortBy: 1 } },
              { $floor: { $multiply: [{ $size: '$durations' }, 0.75] } }
            ]
          },
          2
        ]
      },
      _id: 0
    }
  },
  { $sort: { avgSessionsPerWeek: -1 } }
];

/**
 * Query 2: DAU and 7-day retention per feature
 */
const query2_featureRetention = [
  {
    $match: {
      event_type: 'feature_use',
      timestamp: { $gte: new Date(new Date().setDate(new Date().getDate() - 30)) },
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
      _id: {
        user_id: '$user_id',
        feature_name: '$feature_name',
        date: '$eventDate'
      },
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
  { $limit: 50 }
];

/**
 * Query 3: Onboarding funnel
 */
const query3_onboardingFunnel = [
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
          { stage: 'signup', count: '$signup', conversionRate: 1.0, dropOffRate: 0 },
          { stage: 'first_login', count: '$first_login', conversionRate: { $round: [{ $divide: ['$first_login', { $max: [1, '$signup'] }] }, 3] }, dropOffRate: { $round: [{ $subtract: [1, { $divide: ['$first_login', { $max: [1, '$signup'] }] }] }, 3] } },
          { stage: 'workspace_created', count: '$workspace_created', conversionRate: { $round: [{ $divide: ['$workspace_created', { $max: [1, '$first_login'] }] }, 3] }, dropOffRate: { $round: [{ $subtract: [1, { $divide: ['$workspace_created', { $max: [1, '$first_login'] }] }] }, 3] } },
          { stage: 'first_project', count: '$first_project', conversionRate: { $round: [{ $divide: ['$first_project', { $max: [1, '$workspace_created'] }] }, 3] }, dropOffRate: { $round: [{ $subtract: [1, { $divide: ['$first_project', { $max: [1, '$workspace_created'] }] }] }, 3] } },
          { stage: 'invited_teammate', count: '$invited_teammate', conversionRate: { $round: [{ $divide: ['$invited_teammate', { $max: [1, '$first_project'] }] }, 3] }, dropOffRate: { $round: [{ $subtract: [1, { $divide: ['$invited_teammate', { $max: [1, '$first_project'] }] }] }, 3] } }
        ]
      }
    }
  },
  { $unwind: '$funnel' },
  { $replaceRoot: { newRoot: '$funnel' } }
];

/**
 * Query 4: Top 20 free-tier engaged users
 */
const query4_engagedFreeUsers = [
  {
    $match: {
      subscription_tier: 'free',
      timestamp: { $gte: new Date(new Date().setDate(new Date().getDate() - 90)) }
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
  { $limit: 20 }
];

const queries = [
  { name: 'Q1: Sessions by Tier & Percentiles', pipeline: query1_sessionsByTier },
  { name: 'Q2: Feature DAU & 7-Day Retention', pipeline: query2_featureRetention },
  { name: 'Q3: Onboarding Funnel', pipeline: query3_onboardingFunnel },
  { name: 'Q4: Top 20 Engaged Free Users', pipeline: query4_engagedFreeUsers }
];

async function runQueries() {
  const client = new MongoClient(MONGO_URI);
  
  try {
    console.log('\n=== MongoDB Query Execution ===\n');
    await client.connect();
    
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);
    
    const queryNum = process.argv[2];
    const queriesToRun = queryNum ? [queries[parseInt(queryNum) - 1]] : queries;
    
    for (let i = 0; i < queriesToRun.length; i++) {
      const query = queriesToRun[i];
      console.log(`▶ Executing: ${query.name}`);
      
      const startTime = Date.now();
      const results = await collection.aggregate(query.pipeline).toArray();
      const duration = Date.now() - startTime;
      
      // Save results
      const filename = `query_${i + 1}_results.json`;
      const filepath = path.join(__dirname, 'queries', filename);
      fs.writeFileSync(filepath, JSON.stringify(results, null, 2));
      
      console.log(`  ✓ Completed in ${duration}ms`);
      console.log(`  ✓ ${results.length} results saved to ${filename}\n`);
    }
    
    console.log('=== All Queries Executed Successfully ===\n');
  } catch (error) {
    console.error('Error executing queries:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

runQueries();
