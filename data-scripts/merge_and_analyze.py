#!/usr/bin/env python3
"""
TASK 3: Data Wrangling, Hypothesis Testing & Segmentation
==========================================================

This script performs:
1. SQL + MongoDB data merge on customer/user IDs
2. Comprehensive data cleaning with documentation
3. Hypothesis testing (statistical significance)
4. Customer segmentation (behavioral/value-based/engagement)

Author: Data Analyst
Date: 2026-03-25
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

# Scientific computing & statistics
try:
    from scipy import stats
    from scipy.stats import ttest_ind, chi2_contingency, mannwhitneyu
    import psycopg2
    from pymongo import MongoClient
except ImportError as e:
    print(f"ERROR: Required package not installed: {e}")
    print("Run: pip install psycopg2-binary pymongo scipy pandas numpy")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data-scripts/data_merge_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataWranglingAnalysis:
    """
    Comprehensive data wrangling, merging, and analysis pipeline
    Integrates PostgreSQL (nimbus_core) and MongoDB (nimbus_events)
    """

    def __init__(self):
        """Initialize database connections"""
        self.postgres_conn = None
        self.mongo_client = None
        self.sql_data = {}
        self.mongo_data = {}
        self.merged_data = None
        self.cleaning_log = {}

    # =========================================================================
    # DATABASE CONNECTION
    # =========================================================================

    def connect_postgresql(self):
        """Connect to PostgreSQL nimbus_core database"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=os.getenv('POSTGRES_PORT', 5432),
                database=os.getenv('POSTGRES_DB', 'nimbus_core'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'postgres')
            )
            logger.info('✓ Connected to PostgreSQL (nimbus_core)')
            return True
        except Exception as e:
            logger.error(f'✗ PostgreSQL connection failed: {e}')
            return False

    def connect_mongodb(self):
        """Connect to MongoDB nimbus_events database"""
        try:
            mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client['nimbus_events']
            logger.info('✓ Connected to MongoDB (nimbus_events)')
            return True
        except Exception as e:
            logger.error(f'✗ MongoDB connection failed: {e}')
            return False

    # =========================================================================
    # DATA EXTRACTION
    # =========================================================================

    def extract_sql_data(self):
        """Extract customer and subscription data from PostgreSQL"""
        if not self.postgres_conn:
            logger.error('PostgreSQL connection not established')
            return False

        try:
            cursor = self.postgres_conn.cursor()

            # Extract customers
            logger.info('Extracting customers from PostgreSQL...')
            cursor.execute("""
                SELECT 
                    customer_id,
                    company_name,
                    email,
                    email_domain,
                    created_at,
                    updated_at
                FROM customers
            """)
            self.sql_data['customers'] = pd.DataFrame(
                cursor.fetchall(),
                columns=['customer_id', 'company_name', 'email', 'email_domain', 'created_at', 'updated_at']
            )
            logger.info(f"  → Extracted {len(self.sql_data['customers'])} customers")

            # Extract subscriptions
            logger.info('Extracting subscriptions from PostgreSQL...')
            cursor.execute("""
                SELECT 
                    s.subscription_id,
                    s.customer_id,
                    p.plan_name,
                    p.tier,
                    b.amount as monthly_revenue,
                    s.status,
                    s.start_date,
                    s.end_date,
                    s.created_at
                FROM subscriptions s
                JOIN plans p ON s.plan_id = p.plan_id
                LEFT JOIN billing b ON s.subscription_id = b.subscription_id
            """)
            self.sql_data['subscriptions'] = pd.DataFrame(
                cursor.fetchall(),
                columns=['subscription_id', 'customer_id', 'plan_name', 'tier', 
                        'monthly_revenue', 'status', 'start_date', 'end_date', 'created_at']
            )
            logger.info(f"  → Extracted {len(self.sql_data['subscriptions'])} subscription records")

            # Extract support tickets
            logger.info('Extracting support tickets from PostgreSQL...')
            cursor.execute("""
                SELECT 
                    ticket_id,
                    customer_id,
                    priority,
                    status,
                    created_at,
                    resolved_at
                FROM support_tickets
                WHERE created_at >= CURRENT_DATE - INTERVAL '180 days'
            """)
            self.sql_data['support_tickets'] = pd.DataFrame(
                cursor.fetchall(),
                columns=['ticket_id', 'customer_id', 'priority', 'status', 'created_at', 'resolved_at']
            )
            logger.info(f"  → Extracted {len(self.sql_data['support_tickets'])} support tickets (last 180 days)")

            cursor.close()
            return True

        except Exception as e:
            logger.error(f'Error extracting SQL data: {e}')
            return False

    def extract_mongo_data(self):
        """Extract user activity and event data from MongoDB"""
        try:
            logger.info('Extracting event data from MongoDB...')

            # Extract feature usage events
            events_collection = self.db['events']
            events_data = list(events_collection.find(
                {'timestamp': {'$gte': datetime.utcnow() - timedelta(days=180)}},
                {'_id': 0}
            ))
            self.mongo_data['events'] = pd.DataFrame(events_data)
            logger.info(f"  → Extracted {len(self.mongo_data['events'])} events (last 180 days)")

            # Extract user sessions
            sessions_collection = self.db['sessions']
            sessions_data = list(sessions_collection.find(
                {},
                {'_id': 0}
            ))
            self.mongo_data['sessions'] = pd.DataFrame(sessions_data)
            logger.info(f"  → Extracted {len(self.mongo_data['sessions'])} session records")

            return True

        except Exception as e:
            logger.error(f'Error extracting MongoDB data: {e}')
            return False

    # =========================================================================
    # DATA CLEANING
    # =========================================================================

    def clean_data(self):
        """
        Clean and validate all extracted data
        Document before/after counts and issues
        """
        logger.info('\n=== DATA CLEANING & QUALITY ASSURANCE ===\n')

        # Clean SQL data
        self._clean_sql_customers()
        self._clean_sql_subscriptions()
        self._clean_sql_support_tickets()

        # Clean MongoDB data
        self._clean_mongo_events()
        self._clean_mongo_sessions()

        # Log cleaning summary
        self._log_cleaning_summary()

    def _clean_sql_customers(self):
        """Clean customer data"""
        logger.info('Cleaning CUSTOMERS table...')
        before_count = len(self.sql_data['customers'])

        df = self.sql_data['customers'].copy()

        # Remove duplicates on email
        duplicates_removed = df.duplicated(subset=['email']).sum()
        df = df.drop_duplicates(subset=['email'])

        # Remove null emails
        nulls_removed = df['email'].isna().sum()
        df = df.dropna(subset=['email'])

        # Standardize email domain
        df['email_domain'] = df['email'].str.extract(r'@(.+)$')[0].str.lower()

        after_count = len(df)
        self.sql_data['customers'] = df

        self.cleaning_log['customers'] = {
            'before': before_count,
            'after': after_count,
            'duplicates_removed': duplicates_removed,
            'nulls_removed': nulls_removed,
            'rows_removed': before_count - after_count
        }

        logger.info(f"  ✓ {before_count} → {after_count} rows | "
                   f"Duplicates: {duplicates_removed} | Nulls: {nulls_removed}")

    def _clean_sql_subscriptions(self):
        """Clean subscription data"""
        logger.info('Cleaning SUBSCRIPTIONS table...')
        before_count = len(self.sql_data['subscriptions'])

        df = self.sql_data['subscriptions'].copy()

        # Convert date columns
        date_cols = ['start_date', 'end_date', 'created_at']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Remove invalid date ranges
        invalid_dates = ((df['end_date'].notna()) & (df['end_date'] < df['start_date'])).sum()
        df = df[~((df['end_date'].notna()) & (df['end_date'] < df['start_date']))]

        # Fill missing monthly_revenue with tier average
        tier_avg_revenue = df.groupby('tier')['monthly_revenue'].transform('mean')
        df['monthly_revenue'] = df['monthly_revenue'].fillna(tier_avg_revenue)

        # Remove subscriptions with null start_date or customer_id
        nulls_removed = df[['customer_id', 'start_date']].isna().any(axis=1).sum()
        df = df.dropna(subset=['customer_id', 'start_date'])

        after_count = len(df)
        self.sql_data['subscriptions'] = df

        self.cleaning_log['subscriptions'] = {
            'before': before_count,
            'after': after_count,
            'invalid_date_ranges': invalid_dates,
            'nulls_removed': nulls_removed,
            'revenue_filled': (df['monthly_revenue'].isna()).sum()
        }

        logger.info(f"  ✓ {before_count} → {after_count} rows | "
                   f"Invalid dates: {invalid_dates} | Nulls: {nulls_removed}")

    def _clean_sql_support_tickets(self):
        """Clean support tickets data"""
        logger.info('Cleaning SUPPORT_TICKETS table...')
        before_count = len(self.sql_data['support_tickets'])

        df = self.sql_data['support_tickets'].copy()

        # Convert timestamps
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

        # Remove records with null customer_id
        nulls_removed = df['customer_id'].isna().sum()
        df = df.dropna(subset=['customer_id'])

        # Calculate resolution time
        df['resolution_time_hours'] = (df['resolved_at'] - df['created_at']).dt.total_seconds() / 3600

        after_count = len(df)
        self.sql_data['support_tickets'] = df

        self.cleaning_log['support_tickets'] = {
            'before': before_count,
            'after': after_count,
            'nulls_removed': nulls_removed
        }

        logger.info(f"  ✓ {before_count} → {after_count} rows | Nulls: {nulls_removed}")

    def _clean_mongo_events(self):
        """Clean MongoDB events data"""
        logger.info('Cleaning EVENTS collection...')
        before_count = len(self.mongo_data.get('events', pd.DataFrame()))

        if before_count == 0:
            logger.warning("  ⚠ No events data available")
            return

        df = self.mongo_data['events'].copy()

        # Remove duplicates
        duplicates_removed = df.duplicated().sum()
        df = df.drop_duplicates()

        # Standardize timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        after_count = len(df)
        self.mongo_data['events'] = df

        self.cleaning_log['mongo_events'] = {
            'before': before_count,
            'after': after_count,
            'duplicates_removed': duplicates_removed
        }

        logger.info(f"  ✓ {before_count} → {after_count} records | Duplicates: {duplicates_removed}")

    def _clean_mongo_sessions(self):
        """Clean MongoDB sessions data"""
        logger.info('Cleaning SESSIONS collection...')
        before_count = len(self.mongo_data.get('sessions', pd.DataFrame()))

        if before_count == 0:
            logger.warning("  ⚠ No sessions data available")
            return

        df = self.mongo_data['sessions'].copy()

        # Remove duplicates
        duplicates_removed = df.duplicated().sum()
        df = df.drop_duplicates()

        after_count = len(df)
        self.mongo_data['sessions'] = df

        self.cleaning_log['mongo_sessions'] = {
            'before': before_count,
            'after': after_count,
            'duplicates_removed': duplicates_removed
        }

        logger.info(f"  ✓ {before_count} → {after_count} records | Duplicates: {duplicates_removed}")

    def _log_cleaning_summary(self):
        """Log comprehensive cleaning summary"""
        logger.info('\n=== CLEANING SUMMARY ===')
        total_before = sum(v.get('before', 0) for v in self.cleaning_log.values())
        total_after = sum(v.get('after', 0) for v in self.cleaning_log.values())
        total_removed = total_before - total_after

        logger.info(f"Total rows: {total_before} → {total_after} ({total_removed} removed, "
                   f"{(total_removed/total_before*100):.2f}%)")
        logger.info('Cleaning log saved to: data-scripts/data_merge_analysis.log\n')

    # =========================================================================
    # DATA MERGING
    # =========================================================================

    def merge_data(self):
        """
        Merge SQL and MongoDB data on customer/user IDs
        Create unified dataset for analysis
        """
        logger.info('\n=== DATA MERGING (SQL + MongoDB) ===\n')

        try:
            # Merge customers with subscriptions
            logger.info('Merging customer and subscription data...')
            merged = self.sql_data['customers'].merge(
                self.sql_data['subscriptions'],
                on='customer_id',
                how='left'
            )

            # Merge with support tickets
            logger.info('Merging support tickets...')
            support_summary = self.sql_data['support_tickets'].groupby('customer_id').agg({
                'ticket_id': 'count',
                'priority': lambda x: (x == 'critical').sum(),
                'resolution_time_hours': 'mean'
            }).rename(columns={
                'ticket_id': 'total_tickets',
                'priority': 'critical_tickets',
                'resolution_time_hours': 'avg_resolution_time'
            }).reset_index()

            merged = merged.merge(support_summary, on='customer_id', how='left')
            merged['total_tickets'] = merged['total_tickets'].fillna(0)
            merged['critical_tickets'] = merged['critical_tickets'].fillna(0)

            # Merge with MongoDB events (if available)
            if not self.mongo_data['events'].empty:
                logger.info('Merging MongoDB events...')
                if 'customer_id' in self.mongo_data['events'].columns or 'user_id' in self.mongo_data['events'].columns:
                    events_id_col = 'customer_id' if 'customer_id' in self.mongo_data['events'].columns else 'user_id'
                    event_summary = self.mongo_data['events'].groupby(events_id_col).agg({
                        'event_type': 'count',
                        'timestamp': 'max'
                    }).rename(columns={
                        'event_type': 'total_events',
                        'timestamp': 'last_event_date',
                        events_id_col: 'customer_id'
                    }).reset_index()
                    merged = merged.merge(event_summary, on='customer_id', how='left')
                    merged['total_events'] = merged['total_events'].fillna(0)

            # Merge with sessions (if available)
            if not self.mongo_data['sessions'].empty:
                logger.info('Merging MongoDB sessions...')
                if 'customer_id' in self.mongo_data['sessions'].columns or 'user_id' in self.mongo_data['sessions'].columns:
                    sessions_id_col = 'customer_id' if 'customer_id' in self.mongo_data['sessions'].columns else 'user_id'
                    session_summary = self.mongo_data['sessions'].groupby(sessions_id_col).agg({
                        'duration': 'mean',
                        'session_id': 'count'
                    }).rename(columns={
                        'duration': 'avg_session_duration',
                        'session_id': 'total_sessions',
                        sessions_id_col: 'customer_id'
                    }).reset_index()
                    merged = merged.merge(session_summary, on='customer_id', how='left')
                    merged['total_sessions'] = merged['total_sessions'].fillna(0)

            self.merged_data = merged
            logger.info(f"✓ Merged dataset: {len(self.merged_data)} rows, {len(self.merged_data.columns)} columns\n")
            return True

        except Exception as e:
            logger.error(f'Error merging data: {e}')
            return False

    # =========================================================================
    # HYPOTHESIS TESTING
    # =========================================================================

    def hypothesis_testing(self):
        """
        Formulate and test statistical hypothesis
        
        Hypothesis: Customers with higher engagement (more sessions/events)
        have lower churn rates
        
        H0: Engagement and churn are independent (no relationship)
        H1: Higher engagement is associated with lower churn
        Significance level: α = 0.05
        """
        logger.info('\n=== HYPOTHESIS TESTING ===\n')

        if self.merged_data is None or self.merged_data.empty:
            logger.warning('No merged data available for hypothesis testing')
            return

        try:
            # Prepare data: Define engagement and churn
            df = self.merged_data.copy()

            # Create engagement score
            df['engagement_score'] = (
                df['total_events'].fillna(0) * 0.3 +
                df['total_sessions'].fillna(0) * 0.4 +
                (df['avg_session_duration'].fillna(0) / 60) * 0.3  # Convert to minutes
            )

            # Create churn indicator (based on subscription status or total tickets)
            df['is_churned'] = ((df['status'] == 'cancelled') | 
                               (df['total_tickets'] > df['total_tickets'].quantile(0.75))).astype(int)

            logger.info('HYPOTHESIS:')
            logger.info('  H0: Engagement and churn are independent')
            logger.info('  H1: Higher engagement is associated with lower churn')
            logger.info('  Significance level (α): 0.05')
            logger.info('  Test: Independent samples t-test\n')

            # Separate engaged vs churned customers
            engaged_customers = df[df['engagement_score'] > df['engagement_score'].median()]
            disengaged_customers = df[df['engagement_score'] <= df['engagement_score'].median()]

            engaged_churn_rate = engaged_customers['is_churned'].mean()
            disengaged_churn_rate = disengaged_customers['is_churned'].mean()

            logger.info('DESCRIPTIVE STATISTICS:')
            logger.info(f"  Engaged customers (high engagement):    {len(engaged_customers)} | Churn rate: {engaged_churn_rate:.2%}")
            logger.info(f"  Disengaged customers (low engagement):  {len(disengaged_customers)} | Churn rate: {disengaged_churn_rate:.2%}")

            # Perform t-test
            t_statistic, p_value = ttest_ind(
                engaged_customers['is_churned'].values,
                disengaged_customers['is_churned'].values
            )

            logger.info(f"\nSTATISTICAL TEST RESULTS:')
            logger.info(f"  t-statistic: {t_statistic:.4f}")
            logger.info(f"  p-value: {p_value:.6f}")

            # Interpretation
            alpha = 0.05
            if p_value < alpha:
                logger.info(f"\n✓ RESULT: REJECT H0 (p < {alpha})")
                logger.info('  Conclusion: There IS a statistically significant relationship')
                logger.info('  between engagement and churn. Engaged customers show')
                logger.info(f'  {abs(engaged_churn_rate - disengaged_churn_rate):.2%} lower churn rate.')
            else:
                logger.info(f"\n✗ RESULT: FAIL TO REJECT H0 (p ≥ {alpha})")
                logger.info('  Conclusion: No significant relationship detected.')

            # Effect size (Cohen\'s d)
            cohens_d = (engaged_churn_rate - disengaged_churn_rate) / np.sqrt(
                (engaged_customers['is_churned'].var() + disengaged_customers['is_churned'].var()) / 2
            )
            logger.info(f"\n  Effect size (Cohen\'s d): {cohens_d:.4f}")
            if abs(cohens_d) < 0.2:
                effect_interpretation = "Small effect"
            elif abs(cohens_d) < 0.5:
                effect_interpretation = "Small to medium effect"
            elif abs(cohens_d) < 0.8:
                effect_interpretation = "Medium effect"
            else:
                effect_interpretation = "Large effect"
            logger.info(f"  Interpretation: {effect_interpretation}\n")

        except Exception as e:
            logger.error(f'Error in hypothesis testing: {e}')

    # =========================================================================
    # CUSTOMER SEGMENTATION
    # =========================================================================

    def customer_segmentation(self):
        """
        Create meaningful customer segmentation based on:
        - Revenue/Value (LTV-based)
        - Engagement (activity-based)
        - Behavior (support interactions)
        """
        logger.info('=== CUSTOMER SEGMENTATION ===\n')

        if self.merged_data is None or self.merged_data.empty:
            logger.warning('No merged data available for segmentation')
            return

        try:
            df = self.merged_data.copy()

            # Calculate customer metrics
            df['lifetime_value'] = df['monthly_revenue'].fillna(0) * 12  # Annualized
            df['engagement_score'] = (
                df['total_events'].fillna(0) * 0.3 +
                df['total_sessions'].fillna(0) * 0.4 +
                (df['avg_session_duration'].fillna(0) / 60) * 0.3
            )
            df['support_intensity'] = df['total_tickets'].fillna(0)

            # Define segmentation thresholds
            ltv_high_threshold = df['lifetime_value'].quantile(0.66)
            ltv_medium_threshold = df['lifetime_value'].quantile(0.33)

            engagement_high_threshold = df['engagement_score'].quantile(0.66)
            engagement_medium_threshold = df['engagement_score'].quantile(0.33)

            # Create segments
            def assign_segment(row):
                ltv = row['lifetime_value']
                engagement = row['engagement_score']

                if ltv >= ltv_high_threshold and engagement >= engagement_high_threshold:
                    return 'VIP_ENGAGED'
                elif ltv >= ltv_high_threshold and engagement < engagement_medium_threshold:
                    return 'VIP_AT_RISK'
                elif ltv >= ltv_medium_threshold and engagement >= engagement_high_threshold:
                    return 'GROWTH_POTENTIAL'
                elif ltv < ltv_medium_threshold and engagement >= engagement_high_threshold:
                    return 'ENGAGED_STARTER'
                elif ltv < ltv_medium_threshold and engagement < engagement_medium_threshold:
                    return 'AT_RISK'
                else:
                    return 'STABLE'

            df['customer_segment'] = df.apply(assign_segment, axis=1)

            # Log segmentation results
            logger.info('SEGMENT DEFINITIONS:\n')
            logger.info('  VIP_ENGAGED:        High revenue + High engagement → Retain & expand')
            logger.info('  VIP_AT_RISK:        High revenue + Low engagement → Re-engage')
            logger.info('  GROWTH_POTENTIAL:   Medium revenue + High engagement → Upsell')
            logger.info('  ENGAGED_STARTER:    Low revenue + High engagement → Onboard & grow')
            logger.info('  AT_RISK:            Low revenue + Low engagement → Recover or release')
            logger.info('  STABLE:             Medium revenue + Medium engagement → Maintain\n')

            # Segment statistics
            segment_stats = df.groupby('customer_segment').agg({
                'customer_id': 'count',
                'lifetime_value': ['mean', 'sum'],
                'engagement_score': 'mean',
                'total_tickets': 'mean',
                'tier': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'unknown'
            }).round(2)

            logger.info('SEGMENT STATISTICS:\n')
            for segment, row in segment_stats.iterrows():
                count = int(row[('customer_id', 'count')])
                ltv_mean = row[('lifetime_value', 'mean')]
                ltv_sum = row[('lifetime_value', 'sum')]
                engagement = row[('engagement_score', 'mean')]
                support = row[('total_tickets', 'mean')]
                primary_tier = row[('tier', '<lambda_0>')]

                logger.info(f"  {segment}:")
                logger.info(f"    • Count: {count} customers")
                logger.info(f"    • Avg LTV: ${ltv_mean:,.2f} | Total: ${ltv_sum:,.2f}")
                logger.info(f"    • Avg Engagement: {engagement:.2f}")
                logger.info(f"    • Avg Support Tickets: {support:.2f}")
                logger.info(f"    • Primary tier: {primary_tier}\n')

            self.merged_data = df
            return True

        except Exception as e:
            logger.error(f'Error in customer segmentation: {e}')
            return False

    # =========================================================================
    # EXPORT & SUMMARY
    # =========================================================================

    def export_results(self):
        """Export merged and analyzed data to CSV"""
        try:
            output_dir = 'data-scripts'
            os.makedirs(output_dir, exist_ok=True)

            # Export merged data
            if self.merged_data is not None and not self.merged_data.empty:
                output_file = os.path.join(output_dir, 'merged_analysis_data.csv')
                self.merged_data.to_csv(output_file, index=False)
                logger.info(f'✓ Exported merged data to: {output_file}')

            # Export cleaning log
            log_file = os.path.join(output_dir, 'data_cleaning_log.json')
            with open(log_file, 'w') as f:
                json.dump(self.cleaning_log, f, indent=2, default=str)
            logger.info(f'✓ Exported cleaning log to: {log_file}')

        except Exception as e:
            logger.error(f'Error exporting results: {e}')

    def close_connections(self):
        """Close database connections"""
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info('Closed PostgreSQL connection')

        if self.mongo_client:
            self.mongo_client.close()
            logger.info('Closed MongoDB connection')

    # =========================================================================
    # MAIN EXECUTION
    # =========================================================================

    def run(self):
        """Execute complete analysis pipeline"""
        logger.info('\n╔════════════════════════════════════════════════╗')
        logger.info('║   TASK 3: Data Wrangling & Analysis Pipeline   ║')
        logger.info('╚════════════════════════════════════════════════╝\n')

        # Step 1: Connect to databases
        if not self.connect_postgresql():
            logger.error('Failed to connect to PostgreSQL. Exiting.')
            return False

        if not self.connect_mongodb():
            logger.warning('MongoDB connection failed. Continuing with SQL data only.')

        # Step 2: Extract data
        if not self.extract_sql_data():
            logger.error('Failed to extract SQL data. Exiting.')
            return False

        if self.mongo_client:
            if not self.extract_mongo_data():
                logger.warning('Failed to extract MongoDB data. Continuing with SQL data only.')

        # Step 3: Clean data
        self.clean_data()

        # Step 4: Merge data
        if not self.merge_data():
            logger.error('Failed to merge data. Exiting.')
            return False

        # Step 5: Hypothesis testing
        self.hypothesis_testing()

        # Step 6: Segmentation
        self.customer_segmentation()

        # Step 7: Export results
        self.export_results()

        # Step 8: Cleanup
        self.close_connections()

        logger.info('\n✓ Analysis pipeline completed successfully!')
        return True

if __name__ == '__main__':
    analysis = DataWranglingAnalysis()
    success = analysis.run()
    sys.exit(0 if success else 1)
