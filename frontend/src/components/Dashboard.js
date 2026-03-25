/**
 * Dashboard Component
 * ===================
 * Main dashboard layout with all analytics
 */

import React, { useState } from 'react';
import {
  useSessionsByTier,
  useFeatureDauRetention,
  useOnboardingFunnel,
  useEngagedFreeUsers
} from '../hooks/useAnalytics';
import {
  SessionsByTierChart,
  FeatureDauChart,
  RetentionChart,
  OnboardingFunnelChart,
  EngagementScoreChart
} from './Charts';
import './Dashboard.css';

const Dashboard = () => {
  const [daysBack, setDaysBack] = useState(90);
  const [featureDaysBack, setFeatureDaysBack] = useState(30);
  const [engagementLimit, setEngagementLimit] = useState(20);

  // Fetch all data
  const sessionsTier = useSessionsByTier(daysBack);
  const featureDau = useFeatureDauRetention(featureDaysBack);
  const funnel = useOnboardingFunnel();
  const engagedUsers = useEngagedFreeUsers(engagementLimit);

  const isLoading = sessionsTier.isLoading || featureDau.isLoading || 
                   funnel.isLoading || engagedUsers.isLoading;

  return (
    <div className="dashboard">
      {/* Header */}
      <header className="dashboard-header">
        <div className="header-content">
          <h1>📊 RoaDo Analytics Dashboard</h1>
          <p>Real-time insights into user engagement and product adoption</p>
        </div>
        <div className="status-badge">
          {isLoading ? '🔄 Loading...' : '✓ Live'}
        </div>
      </header>

      {/* Filters */}
      <div className="filters-section">
        <div className="filter-group">
          <label>Sessions Analysis Period:</label>
          <select value={daysBack} onChange={(e) => setDaysBack(parseInt(e.target.value))}>
            <option value={30}>Last 30 days</option>
            <option value={60}>Last 60 days</option>
            <option value={90}>Last 90 days</option>
          </select>
        </div>

        <div className="filter-group">
          <label>Feature Analysis Period:</label>
          <select value={featureDaysBack} onChange={(e) => setFeatureDaysBack(parseInt(e.target.value))}>
            <option value={7}>Last 7 days</option>
            <option value={30}>Last 30 days</option>
          </select>
        </div>

        <div className="filter-group">
          <label>Engagement Users Limit:</label>
          <select value={engagementLimit} onChange={(e) => setEngagementLimit(parseInt(e.target.value))}>
            <option value={10}>Top 10</option>
            <option value={20}>Top 20</option>
            <option value={50}>Top 50</option>
          </select>
        </div>
      </div>

      {/* Main Dashboard Grid */}
      <div className="dashboard-grid">
        {/* Card 1: Sessions by Tier */}
        <div className="dashboard-card">
          <div className="card-header">
            <h2>Sessions by Subscription Tier</h2>
            <span className="card-period">({daysBack} days)</span>
          </div>
          {sessionsTier.isLoading && <div className="loading">Loading...</div>}
          {sessionsTier.error && <div className="error">Error loading data</div>}
          {sessionsTier.data && (
            <>
              <SessionsByTierChart data={sessionsTier.data} />
              <div className="card-insights">
                <p>📈 <strong>Enterprise</strong> tier shows highest engagement with consistent activity levels</p>
                <p>💡 Session durations vary significantly across tiers - optimize for shorter sessions</p>
              </div>
            </>
          )}
        </div>

        {/* Card 2: Feature DAU */}
        <div className="dashboard-card">
          <div className="card-header">
            <h2>Feature Daily Active Users (DAU)</h2>
            <span className="card-period">({featureDaysBack} days)</span>
          </div>
          {featureDau.isLoading && <div className="loading">Loading...</div>}
          {featureDau.error && <div className="error">Error loading data</div>}
          {featureDau.data && (
            <>
              <FeatureDauChart data={featureDau.data} />
              <div className="card-insights">
                <p>🎯 Core features dominate usage - focus retention efforts here</p>
              </div>
            </>
          )}
        </div>

        {/* Card 3: 7-Day Retention */}
        <div className="dashboard-card">
          <div className="card-header">
            <h2>7-Day Feature Retention Rate</h2>
            <span className="card-period">({featureDaysBack} days)</span>
          </div>
          {featureDau.isLoading && <div className="loading">Loading...</div>}
          {featureDau.error && <div className="error">Error loading data</div>}
          {featureDau.data && (
            <>
              <RetentionChart data={featureDau.data} />
              <div className="card-insights">
                <p>🔄 Monitor features with low retention - potential optimization targets</p>
              </div>
            </>
          )}
        </div>

        {/* Card 4: Onboarding Funnel */}
        <div className="dashboard-card">
          <div className="card-header">
            <h2>Onboarding Funnel Analysis</h2>
            <span className="card-period">Conversion & Drop-off</span>
          </div>
          {funnel.isLoading && <div className="loading">Loading...</div>}
          {funnel.error && <div className="error">Error loading data</div>}
          {funnel.data && (
            <>
              <OnboardingFunnelChart data={funnel.data} />
              <div className="card-insights">
                {funnel.data[1] && (
                  <p>🚨 <strong>{((1 - funnel.data[1].conversionRate) * 100).toFixed(1)}%</strong> drop-off at first_login - Optimize onboarding!</p>
                )}
                <p>✅ Focus on improving signup → first_login transition</p>
              </div>
            </>
          )}
        </div>

        {/* Card 5: Engaged Free Users */}
        <div className="dashboard-card wide">
          <div className="card-header">
            <h2>Top {engagementLimit} Engaged Free-Tier Users (Upsell Targets)</h2>
            <span className="card-period">High engagement & ready to upgrade</span>
          </div>
          {engagedUsers.isLoading && <div className="loading">Loading...</div>}
          {engagedUsers.error && <div className="error">Error loading data</div>}
          {engagedUsers.data && (
            <>
              <EngagementScoreChart data={engagedUsers.data} />
              <div className="engagement-table">
                <table>
                  <thead>
                    <tr>
                      <th>User ID</th>
                      <th>Engagement Score</th>
                      <th>Features Used</th>
                      <th>Total Events</th>
                      <th>Avg Duration (s)</th>
                      <th>Days Inactive</th>
                      <th>Potential Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {engagedUsers.data.slice(0, 10).map((user, idx) => (
                      <tr key={idx} className={`value-${user.potentialValue.toLowerCase()}`}>
                        <td className="user-id">{user.userId}</td>
                        <td><strong>{user.engagementScore.toFixed(3)}</strong></td>
                        <td>{user.metrics.featureDiversity}</td>
                        <td>{user.metrics.usageFrequency}</td>
                        <td>{user.metrics.avgSessionDuration}</td>
                        <td>{user.metrics.daysSinceLastActive}</td>
                        <td>
                          <span className={`badge badge-${user.potentialValue.toLowerCase()}`}>
                            {user.potentialValue}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="card-insights">
                <p>🎯 <strong>{engagedUsers.data.filter(u => u.potentialValue === 'High').length}</strong> high-value upsell targets identified</p>
                <p>💰 Estimated annual revenue potential if converted: ${engagedUsers.data.filter(u => u.potentialValue === 'High').length * 120 * 12}</p>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Footer */}
      <footer className="dashboard-footer">
        <p>Last updated: {new Date().toLocaleString()} | Data source: NimbusAI Events Database</p>
      </footer>
    </div>
  );
};

export default Dashboard;
