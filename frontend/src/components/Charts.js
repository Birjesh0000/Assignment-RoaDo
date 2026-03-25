/**
 * Visualization Components
 * ========================
 * Recharts components for dashboard
 */

import React from 'react';
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  Cell
} from 'recharts';

// Sessions by Tier Chart
export const SessionsByTierChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="chart-placeholder">No data available</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 20 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="tier" />
        <YAxis yAxisId="left" label={{ value: 'Avg Sessions/Week', angle: -90, position: 'insideLeft' }} />
        <YAxis yAxisId="right" orientation="right" label={{ value: 'Session Duration (s)', angle: 90, position: 'insideRight' }} />
        <Tooltip />
        <Legend />
        <Bar yAxisId="left" dataKey="avgSessionsPerWeek" fill="#8884d8" name="Avg Sessions/Week" />
        <Bar yAxisId="right" dataKey="percentile_50" fill="#82ca9d" name="Median Duration" />
      </BarChart>
    </ResponsiveContainer>
  );
};

// Feature DAU Chart
export const FeatureDauChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="chart-placeholder">No data available</div>;
  }

  const topFeatures = data.slice(0, 10);

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={topFeatures} layout="vertical" margin={{ top: 5, right: 30, left: 150, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis type="number" />
        <YAxis dataKey="feature" type="category" width={140} tick={{ fontSize: 12 }} />
        <Tooltip />
        <Bar dataKey="dau" fill="#ffc658" name="Daily Active Users" />
      </BarChart>
    </ResponsiveContainer>
  );
};

// Retention Chart
export const RetentionChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="chart-placeholder">No data available</div>;
  }

  const retentionData = data.slice(0, 10).map(d => ({
    ...d,
    retention: (d.retention7Day * 100).toFixed(1)
  }));

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={retentionData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="feature" angle={-45} textAnchor="end" height={80} tick={{ fontSize: 12 }} />
        <YAxis label={{ value: '7-Day Retention %', angle: -90, position: 'insideLeft' }} />
        <Tooltip formatter={(value) => `${(value * 100).toFixed(1)}%`} />
        <Bar dataKey="retention7Day" fill="#82ca9d" name="7-Day Retention" />
      </BarChart>
    </ResponsiveContainer>
  );
};

// Onboarding Funnel Chart
export const OnboardingFunnelChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="chart-placeholder">No data available</div>;
  }

  const funnelData = data.map(stage => ({
    ...stage,
    dropOffPercent: (stage.dropOffRate * 100).toFixed(1)
  }));

  return (
    <ResponsiveContainer width="100%" height={350}>
      <BarChart data={funnelData} margin={{ top: 20, right: 30, left: 0, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="stage" angle={-45} textAnchor="end" height={100} />
        <YAxis yAxisId="left" label={{ value: 'Users', angle: -90, position: 'insideLeft' }} />
        <YAxis yAxisId="right" orientation="right" label={{ value: 'Conversion Rate', angle: 90, position: 'insideRight' }} />
        <Tooltip />
        <Legend />
        <Bar yAxisId="left" dataKey="count" fill="#8884d8" name="User Count" />
        <Bar yAxisId="right" dataKey="conversionRate" fill="#82ca9d" name="Conversion Rate" />
      </BarChart>
    </ResponsiveContainer>
  );
};

// Engagement Score Chart
export const EngagementScoreChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="chart-placeholder">No data available</div>;
  }

  const colors = {
    'High': '#52c41a',
    'Medium': '#faad14',
    'Low': '#f5222d'
  };

  return (
    <div className="engagement-chart-container">
      <ResponsiveContainer width="100%" height={400}>
        <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="userId" angle={-45} textAnchor="end" height={100} tick={{ fontSize: 10 }} />
          <YAxis label={{ value: 'Engagement Score', angle: -90, position: 'insideLeft' }} />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="engagementScore" radius={[8, 8, 0, 0]}>
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={colors[entry.potentialValue] || '#8884d8'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="custom-tooltip" style={{ background: '#fff', padding: '10px', border: '1px solid #ccc' }}>
        <p><strong>{data.userId}</strong></p>
        <p>Score: {data.engagementScore.toFixed(3)}</p>
        <p>Features: {data.metrics.featureDiversity}</p>
        <p>Events: {data.metrics.usageFrequency}</p>
        <p>Avg Duration: {data.metrics.avgSessionDuration}s</p>
        <p>Days Inactive: {data.metrics.daysSinceLastActive}</p>
      </div>
    );
  }
  return null;
};

export default {
  SessionsByTierChart,
  FeatureDauChart,
  RetentionChart,
  OnboardingFunnelChart,
  EngagementScoreChart
};
