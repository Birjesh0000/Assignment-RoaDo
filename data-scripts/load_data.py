#!/usr/bin/env python3
"""
Data Loader - Initialize PostgreSQL and MongoDB with provided data
==================================================================

This script loads data from CSV files into both:
- PostgreSQL (nimbus_core) - SQL data
- MongoDB (nimbus_events) - Event/activity data

Usage: python data-scripts/load_data.py <postgres_csv_dir> <mongo_csv_dir>

The script expects:
PostgreSQL CSVs:
  - customers.csv:          customer_id, company_name, email, email_domain, created_at, updated_at
  - plans.csv:              plan_id, plan_name, tier, base_price, features_included
  - subscriptions.csv:      subscription_id, customer_id, plan_id, previous_plan_id, status, monthly_revenue, ...
  - billing.csv:            billing_id, subscription_id, customer_id, invoice_date, amount
  - support_tickets.csv:    ticket_id, customer_id, subscription_id, title, priority, status, created_at
  - team_members.csv:       team_member_id, customer_id, name, email, role, invited_date
  - feature_flags.csv:      flag_id, customer_id, subscription_id, feature_name, is_enabled

MongoDB CSVs:
  - events.csv:             customer_id, event_type, timestamp, properties
  - sessions.csv:           session_id, customer_id, start_time, end_time, duration
"""

import os
import sys
import json
import csv
import psycopg2
from datetime import datetime
from pymongo import MongoClient
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Load and initialize data into PostgreSQL and MongoDB"""

    def __init__(self):
        self.postgres_conn = None
        self.mongo_db = None

    def connect_postgres(self):
        """Connect to PostgreSQL"""
        try:
            self.postgres_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_PORT', 5432)),
                database=os.getenv('POSTGRES_DB', 'nimbus_core'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'postgres')
            )
            logger.info('✓ Connected to PostgreSQL')
            return True
        except Exception as e:
            logger.error(f'Failed to connect to PostgreSQL: {e}')
            return False

    def connect_mongo(self):
        """Connect to MongoDB"""
        try:
            client = MongoClient(
                os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = client['nimbus_events']
            logger.info('✓ Connected to MongoDB')
            return True
        except Exception as e:
            logger.error(f'Failed to connect to MongoDB: {e}')
            return False

    def load_postgres_data(self, csv_dir):
        """Load PostgreSQL data from CSV files"""
        if not self.postgres_conn:
            logger.error('PostgreSQL not connected')
            return False

        try:
            cursor = self.postgres_conn.cursor()

            postgres_files = {
                'customers.csv': 'customers',
                'plans.csv': 'plans',
                'subscriptions.csv': 'subscriptions',
                'billing.csv': 'billing',
                'support_tickets.csv': 'support_tickets',
                'team_members.csv': 'team_members',
                'feature_flags.csv': 'feature_flags'
            }

            for csv_file, table_name in postgres_files.items():
                csv_path = os.path.join(csv_dir, csv_file)
                if not os.path.exists(csv_path):
                    logger.warning(f'File not found: {csv_path}')
                    continue

                logger.info(f'Loading {table_name} from {csv_file}...')

                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    
                    if rows:
                        columns = list(rows[0].keys())
                        placeholders = ','.join(['%s'] * len(columns))
                        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

                        for row in rows:
                            values = tuple(row.get(col) for col in columns)
                            try:
                                cursor.execute(insert_query, values)
                            except Exception as e:
                                logger.warning(f'Error inserting row: {e}')
                        
                        logger.info(f'  ✓ Loaded {len(rows)} records into {table_name}')

            self.postgres_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            logger.error(f'Error loading PostgreSQL data: {e}')
            self.postgres_conn.rollback()
            return False

    def load_mongo_data(self, csv_dir):
        """Load MongoDB data from CSV files"""
        if not self.mongo_db:
            logger.error('MongoDB not connected')
            return False

        try:
            mongo_files = {
                'events.csv': 'events',
                'sessions.csv': 'sessions'
            }

            for csv_file, collection_name in mongo_files.items():
                csv_path = os.path.join(csv_dir, csv_file)
                if not os.path.exists(csv_path):
                    logger.warning(f'File not found: {csv_path}')
                    continue

                logger.info(f'Loading {collection_name} from {csv_file}...')

                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    documents = list(reader)

                    if documents:
                        collection = self.mongo_db[collection_name]
                        # Clear existing data
                        collection.delete_many({})
                        # Insert new data
                        result = collection.insert_many(documents)
                        logger.info(f'  ✓ Loaded {len(result.inserted_ids)} documents into {collection_name}')

            return True

        except Exception as e:
            logger.error(f'Error loading MongoDB data: {e}')
            return False

    def run(self, postgres_csv_dir, mongo_csv_dir):
        """Run data loading"""
        logger.info('\n╔═══════════════════════════════════╗')
        logger.info('║   Data Loader - Initialize DBs    ║')
        logger.info('╚═══════════════════════════════════╝\n')

        if not self.connect_postgres():
            return False

        if not self.connect_mongo():
            return False

        if not self.load_postgres_data(postgres_csv_dir):
            logger.warning('Failed to load PostgreSQL data')

        if not self.load_mongo_data(mongo_csv_dir):
            logger.warning('Failed to load MongoDB data')

        logger.info('\n✓ Data loading completed!')
        self.close()
        return True

    def close(self):
        """Close connections"""
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info('Closed PostgreSQL connection')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python data-scripts/load_data.py <postgres_csv_dir> <mongo_csv_dir>')
        print('\nExample:')
        print('  python data-scripts/load_data.py data/postgres data/mongo')
        sys.exit(1)

    postgres_dir = sys.argv[1]
    mongo_dir = sys.argv[2]

    if not os.path.isdir(postgres_dir):
        print(f'Error: PostgreSQL CSV directory not found: {postgres_dir}')
        sys.exit(1)

    if not os.path.isdir(mongo_dir):
        print(f'Error: MongoDB CSV directory not found: {mongo_dir}')
        sys.exit(1)

    loader = DataLoader()
    success = loader.run(postgres_dir, mongo_dir)
    sys.exit(0 if success else 1)
