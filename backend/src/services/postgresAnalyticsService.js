/**
 * PostgreSQL Analytics Service Module
 * ===================================
 * Provides methods to execute all 5 complex PostgreSQL queries
 * against the nimbus_core database
 */

const postgres = require('../config/postgres');

class PostgreSQLAnalyticsService {
  /**
   * Q1: Plan Metrics with Support Analysis (Last 6 Months)
   * Returns: plan_name, tier, active_customers, avg_revenue, ticket_rate
   */
  async getPlanMetricsWithSupport() {
    const query = `
      SELECT 
          p.plan_name,
          p.tier,
          COUNT(DISTINCT s.customer_id) AS active_customers,
          ROUND(AVG(s.monthly_revenue), 2) AS avg_monthly_revenue_per_customer,
          COUNT(DISTINCT s.subscription_id) AS total_subscriptions,
          COALESCE(SUM(b.amount), 0) AS six_month_total_revenue,
          COUNT(DISTINCT st.ticket_id) AS total_support_tickets,
          ROUND(
              CAST(COUNT(DISTINCT st.ticket_id) AS DECIMAL) / 
              NULLIF(COUNT(DISTINCT s.customer_id), 0) / 6,
              2
          ) AS support_ticket_rate_per_customer_per_month
      FROM plans p
      LEFT JOIN subscriptions s 
          ON p.plan_id = s.plan_id
          AND s.status = 'active'
          AND s.start_date <= CURRENT_DATE
          AND (s.end_date IS NULL OR s.end_date >= CURRENT_DATE - INTERVAL '6 months')
      LEFT JOIN billing b 
          ON s.subscription_id = b.subscription_id
          AND b.invoice_date >= CURRENT_DATE - INTERVAL '6 months'
      LEFT JOIN support_tickets st 
          ON s.customer_id = st.customer_id
          AND st.created_at >= CURRENT_TIMESTAMP - INTERVAL '6 months'
      GROUP BY p.plan_id, p.plan_name, p.tier
      ORDER BY six_month_total_revenue DESC, active_customers DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch plan metrics',
        details: error.message
      };
    }
  }

  /**
   * Q2: Customer LTV Ranking by Tier with Percentile Analysis
   * Returns: customer details, LTV, tier average LTV, percentage difference, rank
   */
  async getCustomerLTVRankingByTier() {
    const query = `
      WITH customer_ltv AS (
          SELECT 
              c.customer_id,
              c.company_name,
              c.email,
              p.plan_name,
              p.tier,
              COALESCE(SUM(b.amount), 0) AS lifetime_value,
              COUNT(DISTINCT b.invoice_date) AS total_invoices,
              MIN(s.start_date) AS first_subscription_date,
              MAX(s.start_date) AS latest_subscription_date,
              COUNT(DISTINCT s.plan_id) AS num_plan_changes
          FROM customers c
          JOIN subscriptions s ON c.customer_id = s.customer_id
          JOIN plans p ON s.plan_id = p.plan_id
          LEFT JOIN billing b ON s.subscription_id = b.subscription_id
          GROUP BY c.customer_id, c.company_name, c.email, p.plan_id, p.plan_name, p.tier
      ),
      tier_statistics AS (
          SELECT 
              tier,
              AVG(lifetime_value) AS tier_avg_ltv,
              MIN(lifetime_value) AS tier_min_ltv,
              MAX(lifetime_value) AS tier_max_ltv
          FROM customer_ltv
          GROUP BY tier
      )
      SELECT 
          cltv.customer_id,
          cltv.company_name,
          cltv.email,
          cltv.plan_name,
          cltv.tier,
          ROUND(cltv.lifetime_value, 2) AS lifetime_value,
          ROUND(ts.tier_avg_ltv, 2) AS tier_average_ltv,
          ROUND(
              ((cltv.lifetime_value - ts.tier_avg_ltv) / 
              NULLIF(ts.tier_avg_ltv, 0) * 100),
              2
          ) AS ltv_vs_tier_avg_percentage,
          RANK() OVER (PARTITION BY cltv.tier ORDER BY cltv.lifetime_value DESC) AS ltv_rank_in_tier,
          ROUND(
              PERCENT_RANK() OVER (PARTITION BY cltv.tier ORDER BY cltv.lifetime_value DESC) * 100,
              2
          ) AS ltv_percentile,
          cltv.first_subscription_date,
          cltv.total_invoices,
          cltv.num_plan_changes
      FROM customer_ltv cltv
      JOIN tier_statistics ts ON cltv.tier = ts.tier
      ORDER BY cltv.tier, cltv.lifetime_value DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch customer LTV rankings',
        details: error.message
      };
    }
  }

  /**
   * Q3: High-Risk Downgraded Customers (Last 90 Days with 3+ Support Tickets)
   * Returns: downgraded customers with pre-downgrade support ticket data
   */
  async getDowngradedCustomersHighRisk() {
    const query = `
      WITH downgraded_customers AS (
          SELECT 
              s.customer_id,
              c.company_name,
              c.email,
              s.subscription_id,
              s.downgrade_date,
              p_current.plan_name AS current_plan,
              p_current.tier AS current_tier,
              p_prev.plan_name AS previous_plan,
              p_prev.tier AS previous_tier,
              s.monthly_revenue,
              ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.downgrade_date DESC) AS downgrade_recency
          FROM subscriptions s
          JOIN customers c ON s.customer_id = c.customer_id
          JOIN plans p_current ON s.plan_id = p_current.plan_id
          LEFT JOIN plans p_prev ON s.previous_plan_id = p_prev.plan_id
          WHERE s.downgrade_date >= CURRENT_DATE - INTERVAL '90 days'
              AND s.downgrade_date IS NOT NULL
              AND s.previous_plan_id IS NOT NULL
      ),
      support_before_downgrade AS (
          SELECT 
              st.customer_id,
              COUNT(st.ticket_id) AS ticket_count_30d_before,
              MAX(st.created_at) AS last_ticket_before_downgrade
          FROM support_tickets st
          WHERE st.created_at >= CURRENT_TIMESTAMP - INTERVAL '120 days'
          GROUP BY st.customer_id
      )
      SELECT 
          dc.customer_id,
          dc.company_name,
          dc.email,
          dc.downgrade_date,
          dc.previous_plan,
          dc.previous_tier,
          dc.current_plan,
          dc.current_tier,
          COALESCE(sbd.ticket_count_30d_before, 0) AS support_tickets_30d_before_downgrade,
          sbd.last_ticket_before_downgrade,
          ROUND(dc.monthly_revenue, 2) AS subscription_revenue_lost,
          CASE 
              WHEN COALESCE(sbd.ticket_count_30d_before, 0) > 3 THEN 'HIGH_RISK'
              WHEN COALESCE(sbd.ticket_count_30d_before, 0) >= 2 THEN 'MEDIUM_RISK'
              ELSE 'LOW_RISK'
          END AS risk_category,
          DATE_PART('day', CURRENT_DATE - dc.downgrade_date)::INT AS days_since_downgrade
      FROM downgraded_customers dc
      LEFT JOIN support_before_downgrade sbd 
          ON dc.customer_id = sbd.customer_id
          AND sbd.last_ticket_before_downgrade < dc.downgrade_date
      WHERE dc.downgrade_recency = 1
          AND COALESCE(sbd.ticket_count_30d_before, 0) > 3
      ORDER BY COALESCE(sbd.ticket_count_30d_before, 0) DESC, dc.downgrade_date DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch downgraded customers',
        details: error.message
      };
    }
  }

  /**
   * Q4: Month-over-Month Growth and Churn Rate Analysis (Last 24 Months)
   * Returns: monthly metrics with growth rates, churn rates, rolling averages
   */
  async getMonthlyGrowthAndChurnAnalysis() {
    const query = `
      WITH monthly_subscriptions AS (
          SELECT 
              DATE_TRUNC('month', s.start_date)::DATE AS subscription_month,
              p.tier,
              COUNT(DISTINCT s.subscription_id) AS new_subscriptions,
              COALESCE(SUM(s.monthly_revenue), 0) AS monthly_revenue
          FROM subscriptions s
          JOIN plans p ON s.plan_id = p.plan_id
          WHERE s.start_date >= CURRENT_DATE - INTERVAL '24 months'
          GROUP BY DATE_TRUNC('month', s.start_date)::DATE, p.tier
      ),
      monthly_churn AS (
          SELECT 
              DATE_TRUNC('month', s.cancellation_date)::DATE AS churn_month,
              p.tier,
              COUNT(DISTINCT s.subscription_id) AS churned_subscriptions
          FROM subscriptions s
          JOIN plans p ON s.plan_id = p.plan_id
          WHERE s.cancellation_date IS NOT NULL
              AND s.cancellation_date >= CURRENT_DATE - INTERVAL '24 months'
          GROUP BY DATE_TRUNC('month', s.cancellation_date)::DATE, p.tier
      ),
      combined_metrics AS (
          SELECT 
              COALESCE(ms.subscription_month, mc.churn_month) AS metric_month,
              COALESCE(ms.tier, mc.tier) AS tier,
              COALESCE(ms.new_subscriptions, 0) AS new_subscriptions,
              COALESCE(mc.churned_subscriptions, 0) AS churned_subscriptions,
              COALESCE(ms.monthly_revenue, 0) AS monthly_revenue
          FROM monthly_subscriptions ms
          FULL OUTER JOIN monthly_churn mc 
              ON ms.subscription_month = mc.churn_month
              AND ms.tier = mc.tier
      )
      SELECT 
          cm.metric_month,
          cm.tier,
          cm.new_subscriptions,
          cm.churned_subscriptions,
          ROUND(
              ((cm.new_subscriptions - LAG(cm.new_subscriptions) OVER (
                  PARTITION BY cm.tier ORDER BY cm.metric_month
              )) / NULLIF(
                  LAG(cm.new_subscriptions) OVER (PARTITION BY cm.tier ORDER BY cm.metric_month), 
                  0
              ) * 100),
              2
          ) AS mom_growth_rate_percent,
          ROUND(
              AVG(CAST(cm.churned_subscriptions AS DECIMAL)) OVER (
                  PARTITION BY cm.tier 
                  ORDER BY cm.metric_month 
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
              ),
              2
          ) AS rolling_3month_avg_churn,
          CASE 
              WHEN cm.churned_subscriptions > 2 * COALESCE(
                  AVG(CAST(cm.churned_subscriptions AS DECIMAL)) OVER (
                      PARTITION BY cm.tier 
                      ORDER BY cm.metric_month 
                      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                  ), 0
              ) THEN 'ANOMALY_ALERT'
              ELSE 'NORMAL'
          END AS churn_anomaly_flag,
          ROUND(cm.monthly_revenue, 2) AS monthly_revenue
      FROM combined_metrics cm
      ORDER BY cm.metric_month DESC, cm.tier
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch growth and churn analysis',
        details: error.message
      };
    }
  }

  /**
   * Q5: Potential Duplicate Customer Detection
   * Returns: pairs of customers likely to be duplicates with risk scoring
   */
  async getDuplicateCustomerDetection() {
    const query = `
      WITH customer_similarities AS (
          SELECT 
              c1.customer_id AS customer_id_1,
              c2.customer_id AS customer_id_2,
              c1.company_name AS company_name_1,
              c2.company_name AS company_name_2,
              c1.email AS email_1,
              c2.email AS email_2,
              c1.email_domain AS domain_1,
              c2.email_domain AS domain_2,
              ROUND(
                  CAST(SUM(
                      CASE WHEN SUBSTRING(LOWER(c1.company_name), pos, 1) = 
                                 SUBSTRING(LOWER(c2.company_name), pos, 1)
                           THEN 1 ELSE 0 END
                  ) AS DECIMAL) / 
                  NULLIF(GREATEST(LENGTH(c1.company_name), LENGTH(c2.company_name)), 0) * 100,
                  2
              ) AS name_similarity_score
          FROM customers c1
          CROSS JOIN customers c2
          WHERE c1.customer_id < c2.customer_id
              AND (c1.email_domain = c2.email_domain OR 
                   LOWER(c1.company_name) LIKE CONCAT('%', LOWER(c2.company_name), '%') OR
                   LOWER(c2.company_name) LIKE CONCAT('%', LOWER(c1.company_name), '%'))
      ),
      team_member_overlaps AS (
          SELECT 
              t1.customer_id AS customer_id_1,
              t2.customer_id AS customer_id_2,
              t1.email AS shared_email,
              COUNT(t1.email) AS shared_email_count
          FROM team_members t1
          JOIN team_members t2 ON t1.email = t2.email AND t1.customer_id < t2.customer_id
          GROUP BY t1.customer_id, t2.customer_id, t1.email
      )
      SELECT 
          COALESCE(cs.customer_id_1, tmo.customer_id_1) AS customer_id_1,
          COALESCE(cs.customer_id_2, tmo.customer_id_2) AS customer_id_2,
          cs.company_name_1,
          cs.company_name_2,
          cs.domain_1,
          cs.domain_2,
          cs.name_similarity_score,
          COUNT(DISTINCT tmo.shared_email) AS shared_team_members,
          STRING_AGG(DISTINCT tmo.shared_email, ', ') AS shared_email_list,
          CASE 
              WHEN cs.domain_1 = cs.domain_2 AND COUNT(DISTINCT tmo.shared_email) >= 1 THEN 'VERY_HIGH_RISK'
              WHEN cs.name_similarity_score >= 80 AND COUNT(DISTINCT tmo.shared_email) >= 1 THEN 'HIGH_RISK'
              WHEN cs.domain_1 = cs.domain_2 AND cs.name_similarity_score >= 70 THEN 'HIGH_RISK'
              WHEN cs.name_similarity_score >= 90 THEN 'MEDIUM_RISK'
              WHEN COUNT(DISTINCT tmo.shared_email) >= 2 THEN 'MEDIUM_RISK'
              ELSE 'LOW_RISK'
          END AS duplicate_risk_score
      FROM customer_similarities cs
      FULL OUTER JOIN team_member_overlaps tmo 
          ON cs.customer_id_1 = tmo.customer_id_1 AND cs.customer_id_2 = tmo.customer_id_2
      GROUP BY 
          COALESCE(cs.customer_id_1, tmo.customer_id_1),
          COALESCE(cs.customer_id_2, tmo.customer_id_2),
          cs.company_name_1,
          cs.company_name_2,
          cs.domain_1,
          cs.domain_2,
          cs.name_similarity_score
      HAVING CASE 
          WHEN cs.domain_1 = cs.domain_2 AND COUNT(DISTINCT tmo.shared_email) >= 1 THEN TRUE
          WHEN cs.name_similarity_score >= 80 AND COUNT(DISTINCT tmo.shared_email) >= 1 THEN TRUE
          WHEN cs.domain_1 = cs.domain_2 AND cs.name_similarity_score >= 70 THEN TRUE
          WHEN cs.name_similarity_score >= 90 THEN TRUE
          WHEN COUNT(DISTINCT tmo.shared_email) >= 2 THEN TRUE
          ELSE FALSE
      END
      ORDER BY duplicate_risk_score DESC, shared_team_members DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch duplicate customers',
        details: error.message
      };
    }
  }

  /**
   * Utility: Get all customers
   */
  async getAllCustomers() {
    const query = `
      SELECT 
          customer_id,
          company_name,
          email,
          email_domain,
          created_at,
          updated_at
      FROM customers
      ORDER BY created_at DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        count: result.length,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch customers',
        details: error.message
      };
    }
  }

  /**
   * Utility: Get subscription statistics
   */
  async getSubscriptionStatistics() {
    const query = `
      SELECT 
          p.tier,
          COUNT(DISTINCT s.subscription_id) AS total_subscriptions,
          COUNT(DISTINCT CASE WHEN s.status = 'active' THEN s.subscription_id END) AS active_subscriptions,
          COUNT(DISTINCT CASE WHEN s.status = 'cancelled' THEN s.subscription_id END) AS cancelled_subscriptions,
          ROUND(AVG(s.monthly_revenue), 2) AS avg_monthly_revenue,
          ROUND(SUM(s.monthly_revenue), 2) AS total_monthly_revenue
      FROM subscriptions s
      JOIN plans p ON s.plan_id = p.plan_id
      GROUP BY p.tier
      ORDER BY p.tier DESC
    `;

    try {
      const result = await postgres.query(query);
      return {
        success: true,
        data: result
      };
    } catch (error) {
      throw {
        success: false,
        error: 'Failed to fetch subscription statistics',
        details: error.message
      };
    }
  }
}

module.exports = new PostgreSQLAnalyticsService();
