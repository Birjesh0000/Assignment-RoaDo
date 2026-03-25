#!/usr/bin/env python3
"""
Power BI Setup Guide & Data Export
===================================
Generates data exports and configuration guide for Power BI dashboard creation
"""

import os
import json
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerBISetupGuide:
    """Generate Power BI setup documentation and exports"""

    def __init__(self, output_dir: str = 'powerbi'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def create_power_bi_guide(self):
        """Create comprehensive Power BI setup guide"""
        guide = """
# Power BI Dashboard Setup Guide
================================

## Overview
Create an interactive Power BI dashboard combining PostgreSQL (CRM) and MongoDB (Events) data.
Dashboard should tell a cohesive story for VP of Product and Head of Customer Success.

---

## Step 1: Data Source Connections

### PostgreSQL Connection (nimbus_core)
1. In Power BI Desktop: Get Data → PostgreSQL Database
2. Server: localhost
3. Database: nimbus_core
4. Username: postgres
5. Password: postgres
6. Load tables:
   - customers
   - subscriptions
   - plans
   - billing
   - support_tickets

### MongoDB Connection (nimbus_events)
1. Get Data → MongoDB (if available) or Extract to CSV
2. Server: localhost:27017
3. Database: nimbus_events
4. Collections to load:
   - events
   - sessions

---

## Step 2: Data Modeling

### Relationships to Create:
- customers ←→ subscriptions (on customer_id)
- subscriptions ←→ plans (on plan_id)
- subscriptions ←→ billing (on subscription_id)
- customers ←→ support_tickets (on customer_id)
- events ←→ customers (on customer_id)

### Calculated Columns:
```DAX
// LTV (Lifetime Value)
LTV = SUMIF(Billing[customer_id], Customers[customer_id], Billing[amount])

// Engagement Score (from MongoDB)
EngagementScore = [TotalEvents] * 0.3 + [TotalSessions] * 0.4 + [AvgSessionDuration] * 0.3

// Churn Risk
ChurnRisk = IF([Status] = "cancelled", 1, IF([SupportTickets] > 5, 1, 0))

// Tier Revenue
TierRevenue = SUMIF(Subscriptions[tier], RPT_PlanMetrics[tier], RPT_PlanMetrics[total_revenue])
```

---

## Step 3: Visualizations (5+ Required)

### Visualization 1: Plan Performance (Bar Chart)
- Title: "Plan Performance - Last 6 Months"
- X-Axis: Plan Tier (free, pro, enterprise)
- Y-Axis: Active Customers count
- Secondary Y-Axis: Average Monthly Revenue
- Color: By Tier
- Business Question: Which plan tier drives most revenue and engagement?

### Visualization 2: Customer LTV Distribution (Scatter Plot)
- Title: "Customer LTV by Tier & Engagement"
- X-Axis: Engagement Score (MongoDB)
- Y-Axis: Lifetime Value (PostgreSQL)
- Color: Plan Tier
- Size: Support Tickets
- Business Question: How does engagement correlate with revenue?

### Visualization 3: Growth & Churn Trends (Line Chart)
- Title: "Monthly Subscription Growth & Churn Rate (12 months)"
- Line 1: New Subscriptions (primary axis)
- Line 2: Churn Rate % (secondary axis)
- Color: Green (growth), Red (churn)
- Business Question: Are we growing? What's our churn trend?

### Visualization 4: Support Intensity (Table)
- Title: "High-Risk Customers (Downgraded with High Support)"
- Columns: Customer, Previous Plan, Current Plan, Tickets, Revenue Lost
- Row Count: Top 20
- Color: Risk Score (red for high)
- Business Question: Who needs immediate re-engagement?

### Visualization 5: Customer Segments (Pie/Donut Chart)
- Title: "Customer Segmentation Distribution"
- Segments: VIP_ENGAGED, VIP_AT_RISK, GROWTH_POTENTIAL, ENGAGED_STARTER, AT_RISK, STABLE
- Size: Customer Count
- Color: Green (engaged), Yellow (at-risk), Blue (potential)
- Business Question: What % of customers are in each segment?

### Bonus Visualization 6: Engagement vs Revenue (Combined - SQL + MongoDB)
- Title: "Engagement-Revenue Correlation"
- Scatter plot combining:
  - X: Total Events + Sessions (MongoDB)
  - Y: Monthly Revenue (PostgreSQL)
  - Color: Churn Status
  - Size: Support Tickets
- Business Question: Are engaged users more valuable?

---

## Step 4: Interactive Filters (2+ Required)

### Filter 1: Date Range Selector
- Type: Date range picker
- Apply to: All visualizations
- Default: Last 12 months

### Filter 2: Plan Tier (Slicer)
- Type: Dropdown/Slicer
- Values: Free, Pro, Enterprise, All
- Apply to: Plan Performance, Growth Trends, Segments

### Optional Filter 3: Churn Status
- Type: Buttons (Active/Churned/All)
- Apply to: Customer-level visualizations

---

## Step 5: Dashboard Story & Recommendations

### Page 1: Executive Summary (Title: "Revenue Health")
- Top KPIs:
  - Total Active Customers (card)
  - AUM (Annual Value Multiplier on MRR)
  - Churn Rate (% highlighted red if >5%)
  - Avg Customer LTV (tier-based average)
- Chart 1: Growth & Churn Trends
- Chart 2: Plan Performance
- Key Insight: "Enterprise tier drives 60% of revenue but has 3% churn"

### Page 2: Engagement Deep Dive (Title: "User Engagement Insights")
- Chart 1: Engagement vs Revenue scatter
- Chart 2: Feature Adoption by Tier
- Chart 3: Session Duration Trends
- Key Insight: "Feature X users have 40% lower churn"

### Page 3: Risk Management (Title: "Retention Risks")
- Chart 1: High-Risk Downgraded Customers (table)
- Chart 2: Support Tickets by Severity
- Chart 3: Churn Funnel
- Key Insight: "Support tickets increase 30 days BEFORE downgrade"

---

## Step 6: Three Actionable Recommendations

### Recommendation 1: VIP Re-engagement Program
**Segment:** VIP_AT_RISK (High revenue, low engagement)
**Action:** Implement personalized outreach, quarterly business reviews
**Expected Impact:** Reduce downgrade by 25%, recover $50K+ ARR
**Timeline:** 30 days to implement
**KPI:** Decreased support tickets, increased feature adoption

### Recommendation 2: Upsell to Growth Potential
**Segment:** GROWTH_POTENTIAL (Mid revenue, high engagement)
**Action:** Targeted upsell campaigns for additional features/users
**Expected Impact:** 15-20% revenue increase per customer
**Timeline:** Ongoing campaign, measure quarterly
**KPI:** Feature adoption rate, ACV increase

### Recommendation 3: Quality Improvement Initiative
**Root Cause:** Support tickets spike 30 days before downgrade
**Action:** Implement proactive support, feature training, health checks
**Expected Impact:** Reduce downgrade-related churn by 40%
**Timeline:** 60 days to establish program
**KPI:** Ticket resolution time, customer satisfaction score

---

## Step 7: Formatting & Polish

### Design Elements:
- Color Scheme: Professional (Blue, Green, Gray, Red for alerts)
- Fonts: Segoe UI (standard), size 11-14pt
- Logo: Company logo in top-left corner
- Theme: Modern, minimal

### Annotations:
- Add text boxes with business insights near each chart
- Highlight anomalies or trends
- Include trend arrows (↑ up, ↓ down)

### Interactivity:
- Filters at top, affecting all charts below
- Drill-down capability (by tier → by plan → by customer)
- Hover tooltips with additional context

---

## Step 8: Export & Share

### For Submission:
1. Save as: powerbi/dashboard.pbix
2. Create readonly PDF export: powerbi/dashboard.pdf (File → Export)
3. Take screenshot: powerbi/dashboard_screenshot.png (for reference)

### For Sharing:
- Publish to Power BI Service (if available)
- Set up refresh schedule (daily for real data)
- Share with stakeholders: VP Product, Head of Customer Success

---

## Troubleshooting

### Data Not Loading?
- Check database connections (localhost:5432, localhost:27017)
- Verify tables exist in both PostgreSQL and MongoDB
- Check data types match (dates should be datetime, not strings)

### Performance Issues?
- Aggregate large tables (events, sessions) monthly
- Create summary tables for year-over-year comparisons
- Use DirectQuery for large datasets

### Can't See MongoDB Data?
- Consider exporting MongoDB to CSV first
- Use Python script to merge data
- Create views in PostgreSQL of MongoDB summaries

---

End of Guide
"""

        filepath = os.path.join(self.output_dir, 'POWERBI_SETUP_GUIDE.txt')
        with open(filepath, 'w') as f:
            f.write(guide)
        logger.info(f'✓ Created Power BI setup guide: {filepath}')

    def create_json_config(self):
        """Create JSON configuration for Power BI structure"""
        config = {
            "dashboard_name": "RoaDo Analytics Dashboard",
            "pages": [
                {
                    "page_id": "page_1",
                    "title": "Revenue Health",
                    "description": "KPIs and revenue metrics by tier",
                    "visualizations": [
                        {
                            "viz_id": "kpi_active_customers",
                            "type": "Card",
                            "title": "Total Active Customers",
                            "data_source": "PostgreSQL",
                            "query": "SELECT COUNT(DISTINCT customer_id) FROM subscriptions WHERE status = 'active'"
                        },
                        {
                            "viz_id": "growth_churn",
                            "type": "Line Chart",
                            "title": "Monthly Growth & Churn Rate",
                            "x_axis": "Month",
                            "y_axis_primary": "New Subscriptions",
                            "y_axis_secondary": "Churn Rate %",
                            "data_source": "PostgreSQL"
                        },
                        {
                            "viz_id": "plan_performance",
                            "type": "Bar Chart",
                            "title": "Plan Performance (6 Months)",
                            "x_axis": "Plan Tier",
                            "y_axis": "Active Customers",
                            "secondary_y_axis": "Avg Monthly Revenue",
                            "data_source": "PostgreSQL"
                        }
                    ]
                },
                {
                    "page_id": "page_2",
                    "title": "Engagement & Retention",
                    "description": "User engagement from MongoDB correlated with email from SQL",
                    "visualizations": [
                        {
                            "viz_id": "engagement_revenue",
                            "type": "Scatter Plot",
                            "title": "Engagement vs Revenue Correlation",
                            "x_axis": "Total Events + Sessions",
                            "y_axis": "Lifetime Value",
                            "color": "Churn Status",
                            "size": "Support Tickets",
                            "data_source": "MongoDB + PostgreSQL (Combined)"
                        },
                        {
                            "viz_id": "ltv_ranking",
                            "type": "Scatter Plot",
                            "title": "Customer LTV Distribution by Tier",
                            "x_axis": "Engagement Score",
                            "y_axis": "Lifetime Value",
                            "color": "Plan Tier",
                            "size": "Support Tickets",
                            "data_source": "PostgreSQL + MongoDB"
                        }
                    ]
                },
                {
                    "page_id": "page_3",
                    "title": "Risk Management",
                    "description": "Identify at-risk customers and support issues",
                    "visualizations": [
                        {
                            "viz_id": "high_risk_customers",
                            "type": "Table",
                            "title": "High-Risk Downgraded Customers (90 days)",
                            "columns": ["Customer", "Previous Plan", "Current Plan", "Support Tickets", "Revenue Lost"],
                            "row_limit": 20,
                            "data_source": "PostgreSQL"
                        },
                        {
                            "viz_id": "customer_segments",
                            "type": "Pie Chart",
                            "title": "Customer Segmentation",
                            "segments": ["VIP_ENGAGED", "VIP_AT_RISK", "GROWTH_POTENTIAL", "ENGAGED_STARTER", "AT_RISK", "STABLE"],
                            "data_source": "PostgreSQL + MongoDB"
                        }
                    ]
                }
            ],
            "filters": [
                {
                    "filter_id": "date_range",
                    "type": "Date Range",
                    "label": "Period",
                    "default": "Last 12 months"
                },
                {
                    "filter_id": "plan_tier",
                    "type": "Dropdown",
                    "label": "Plan Tier",
                    "values": ["Free", "Pro", "Enterprise", "All"],
                    "default": "All"
                }
            ],
            "recommendations": [
                {
                    "recommendation_id": "rec_1",
                    "title": "VIP Re-engagement Program",
                    "segment": "VIP_AT_RISK",
                    "action": "Implement personalized outreach, quarterly business reviews",
                    "expected_impact": "Reduce downgrade by 25%, recover $50K+ ARR",
                    "kpi": "Decreased support tickets, increased feature adoption"
                },
                {
                    "recommendation_id": "rec_2",
                    "title": "Upsell to Growth Potential",
                    "segment": "GROWTH_POTENTIAL",
                    "action": "Targeted upsell campaigns for additional features/users",
                    "expected_impact": "15-20% revenue increase per customer",
                    "kpi": "Feature adoption rate, ACV increase"
                },
                {
                    "recommendation_id": "rec_3",
                    "title": "Quality Improvement Initiative",
                    "root_cause": "Support tickets spike 30 days before downgrade",
                    "action": "Implement proactive support, feature training, health checks",
                    "expected_impact": "Reduce downgrade-related churn by 40%",
                    "kpi": "Ticket resolution time, customer satisfaction score"
                }
            ]
        }

        filepath = os.path.join(self.output_dir, 'powerbi_structure.json')
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f'✓ Created Power BI JSON config: {filepath}')

    def run(self):
        """Generate all Power BI setup files"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Power BI Setup Guide Generator    ║')
        logger.info('╚═════════════════════════════════════╝\n')

        self.create_power_bi_guide()
        self.create_json_config()

        logger.info(f'\n✓ Power BI setup files generated in: {os.path.abspath(self.output_dir)}\n')
        logger.info('Next steps:')
        logger.info(f'  1. Read: {self.output_dir}/POWERBI_SETUP_GUIDE.txt')
        logger.info(f'  2. Reference: {self.output_dir}/powerbi_structure.json')
        logger.info('  3. Create dashboard in Power BI Desktop\n')


if __name__ == '__main__':
    setup = PowerBISetupGuide()
    setup.run()
