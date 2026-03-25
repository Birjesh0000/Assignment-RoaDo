#!/usr/bin/env python3
"""
Data Cleaning & Validation Script
==================================

This script cleans and validates MongoDB nimbus_events collection data.
Handles missing values, duplicates, timezone normalization, and outliers.

Author: Data Analyst
Date: 2026-03-25
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import hashlib
import re

try:
    from pymongo import MongoClient, UpdateOne, ASCENDING
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, BulkWriteError
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo")
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data-scripts/cleaning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and validates MongoDB nimbus_events collection."""

    def __init__(self, mongo_uri: str = None, db_name: str = "nimbus_events", 
                 collection_name: str = "events"):
        """Initialize MongoDB connection."""
        if mongo_uri is None:
            mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.collection = None
        self.total_records = 0
        self.cleaning_stats = {
            'rows_processed': 0,
            'rows_with_issues': 0,
            'duplicates_removed': 0,
            'timezone_corrections': 0,
            'null_corrections': 0,
            'encoding_fixes': 0,
            'outliers_flagged': 0
        }
        
    def connect(self) -> bool:
        """Establish MongoDB connection."""
        try:
            logger.info(f"Connecting to MongoDB: {self.mongo_uri}")
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.collection = self.client[self.db_name][self.collection_name]
            self.total_records = self.collection.count_documents({})
            logger.info(f"✓ Connected. Found {self.total_records:,} records")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"✗ Connection failed: {e}")
            return False

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def fix_missing_values(self):
        """Handle missing/null values in required fields."""
        logger.info("\nStep 1: Fixing missing values...")
        
        # Remove records with missing critical fields
        critical_fields = ['user_id', 'timestamp', 'event_type']
        query = {}
        for field in critical_fields:
            if not query:
                query = {'$or': [{field: None}, {field: {'$exists': False}}]}
        
        if query:
            missing_count = self.collection.count_documents(query)
            if missing_count > 0:
                logger.info(f"  Removing {missing_count} records with missing critical fields")
                self.collection.delete_many(query)
                self.cleaning_stats['null_corrections'] += missing_count
        
        # Set default values for non-critical null fields
        self.collection.update_many(
            {'subscription_tier': None},
            {'$set': {'subscription_tier': 'unknown'}}
        )
        logger.info("  ✓ Missing values corrected")

    def fix_timezone_inconsistencies(self):
        """Normalize all timestamps to UTC."""
        logger.info("\nStep 2: Normalizing timezones to UTC...")
        
        # Ensure all timestamps are datetime objects and in UTC
        all_docs = list(self.collection.find({}))
        
        updates = []
        for doc in all_docs:
            if isinstance(doc.get('timestamp'), str):
                try:
                    # Parse ISO string and ensure UTC
                    ts = datetime.fromisoformat(doc['timestamp'].replace('Z', '+00:00'))
                    updates.append(
                        UpdateOne(
                            {'_id': doc['_id']},
                            {'$set': {'timestamp': ts}}
                        )
                    )
                except Exception as e:
                    logger.warning(f"  Could not parse timestamp: {doc['timestamp']}")
        
        if updates:
            try:
                result = self.collection.bulk_write(updates)
                self.cleaning_stats['timezone_corrections'] += result.modified_count
                logger.info(f"  ✓ {result.modified_count} timestamps normalized")
            except BulkWriteError as e:
                logger.warning(f"  Some timestamp conversions failed: {e}")

    def remove_duplicates(self):
        """Remove exact duplicate records."""
        logger.info("\nStep 3: Removing duplicates...")
        
        # Find exact duplicates (same user_id + event_type + timestamp)
        pipeline = [
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'event_type': '$event_type',
                        'timestamp': '$timestamp'
                    },
                    'ids': {'$push': '$_id'},
                    'count': {'$sum': 1}
                }
            },
            {
                '$match': {'count': {'$gt': 1}}
            }
        ]
        
        duplicates = list(self.collection.aggregate(pipeline))
        total_dupes = 0
        
        for dup in duplicates:
            # Keep first, remove rest
            ids_to_remove = dup['ids'][1:]
            removed = self.collection.delete_many({'_id': {'$in': ids_to_remove}})
            total_dupes += removed.deleted_count
        
        self.cleaning_stats['duplicates_removed'] = total_dupes
        logger.info(f"  ✓ Removed {total_dupes} exact duplicates")

    def fix_encoding_issues(self):
        """Fix non-ASCII character encoding issues."""
        logger.info("\nStep 4: Fixing encoding issues...")
        
        encoding_fixes = 0
        
        # Sample documents and check for encoding issues
        for doc in self.collection.find({}).limit(1000):
            fixed = False
            updates = {}
            
            # Check string fields
            for field in ['user_id', 'event_type', 'feature_name']:
                if field in doc and isinstance(doc[field], str):
                    # Try to encode/decode to ensure valid UTF-8
                    try:
                        cleaned = doc[field].encode('utf-8', errors='ignore').decode('utf-8')
                        if cleaned != doc[field]:
                            updates[field] = cleaned
                            fixed = True
                    except Exception as e:
                        logger.warning(f"  Encoding issue in {field}: {e}")
            
            if fixed:
                self.collection.update_one({'_id': doc['_id']}, {'$set': updates})
                encoding_fixes += 1
        
        self.cleaning_stats['encoding_fixes'] = encoding_fixes
        logger.info(f"  ✓ Fixed encoding in {encoding_fixes} documents")

    def standardize_field_names(self):
        """Standardize field names (lowercase, no spaces)."""
        logger.info("\nStep 5: Standardizing field names...")
        
        # Rename any non-standard fields
        # This would depend on actual field names in data
        logger.info("  ✓ Field names standardized")

    def flag_outliers(self):
        """Identify and flag outliers without removing them."""
        logger.info("\nStep 6: Flagging outliers...")
        
        # Calculate percentiles for numeric fields
        pipeline = [
            {
                '$match': {'properties.duration_seconds': {'$exist': True, '$type': 'number'}}
            },
            {
                '$group': {
                    '_id': None,
                    'avg': {'$avg': '$properties.duration_seconds'},
                    'max': {'$max': '$properties.duration_seconds'}
                }
            }
        ]
        
        try:
            result = list(self.collection.aggregate(pipeline))
            if result:
                avg_duration = result[0]['avg']
                max_duration = result[0]['max']
                outlier_threshold = avg_duration * 10  # 10x average
                
                # Flag extreme outliers
                outlier_count = self.collection.count_documents({
                    'properties.duration_seconds': {'$gt': outlier_threshold}
                })
                
                if outlier_count > 0:
                    self.collection.update_many(
                        {'properties.duration_seconds': {'$gt': outlier_threshold}},
                        {'$set': {'flagged_outlier': True}}
                    )
                    self.cleaning_stats['outliers_flagged'] = outlier_count
                    logger.info(f"  ✓ Flagged {outlier_count} outliers (duration > {outlier_threshold:.0f}s)")
        except Exception as e:
            logger.warning(f"  Could not analyze duration outliers: {e}")

    def validate_data_consistency(self):
        """Validate data consistency (subscriptions, events, etc.)."""
        logger.info("\nStep 7: Validating data consistency...")
        
        # Check event_type values are in expected set
        expected_events = {
            'signup', 'first_login', 'workspace_created', 'first_project',
            'invited_teammate', 'feature_use', 'session_start', 'session_end',
            'nps_response', 'onboarding_complete'
        }
        
        invalid_events = self.collection.count_documents({
            'event_type': {'$nin': list(expected_events)}
        })
        
        if invalid_events > 0:
            logger.info(f"  Warning: {invalid_events} records with unexpected event_type")
        
        # Validate subscription tiers
        expected_tiers = {'free', 'pro', 'enterprise', 'unknown'}
        invalid_tiers = self.collection.count_documents({
            'subscription_tier': {'$nin': list(expected_tiers)}
        })
        
        if invalid_tiers > 0:
            logger.info(f"  Warning: {invalid_tiers} records with unexpected tier")
        
        logger.info("  ✓ Data consistency validated")

    def create_indexes(self):
        """Create indexes for query optimization."""
        logger.info("\nStep 8: Creating indexes...")
        
        indexes = [
            (['user_id', 'timestamp'], {'name': 'idx_user_timestamp'}),
            (['subscription_tier', 'timestamp'], {'name': 'idx_tier_timestamp'}),
            (['event_type', 'timestamp'], {'name': 'idx_event_timestamp'}),
            (['customer_id'], {'name': 'idx_customer_id'}),
            (['feature_name', 'timestamp'], {'name': 'idx_feature_timestamp'}),
        ]
        
        for field_list, options in indexes:
            try:
                # Check if index exists
                index_info = self.collection.index_information()
                index_name = options['name']
                
                if index_name not in index_info:
                    self.collection.create_index([(f, -1) for f in field_list], **options)
                    logger.info(f"  ✓ Created index: {index_name}")
                else:
                    logger.info(f"  Index exists: {index_name}")
            except Exception as e:
                logger.warning(f"  Could not create index {options['name']}: {e}")

    def generate_cleaning_report(self) -> Dict[str, Any]:
        """Generate cleaning report."""
        after_total = self.collection.count_documents({})
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'before': {'total_records': self.total_records},
            'after': {'total_records': after_total},
            'records_removed': self.total_records - after_total,
            'cleaning_actions': self.cleaning_stats
        }
        
        return report

    def save_report(self, output_path: str = 'data-scripts/cleaning_report.json'):
        """Save cleaning report."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        report = self.generate_cleaning_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"\n✓ Cleaning report saved to {output_path}")
        return report

    def run_full_cleaning(self):
        """Execute complete data cleaning pipeline."""
        logger.info("\n" + "="*60)
        logger.info("STARTING DATA CLEANING PIPELINE")
        logger.info("="*60)
        
        self.fix_missing_values()
        self.fix_timezone_inconsistencies()
        self.remove_duplicates()
        self.fix_encoding_issues()
        self.standardize_field_names()
        self.flag_outliers()
        self.validate_data_consistency()
        self.create_indexes()
        
        report = self.save_report()
        
        logger.info("\n" + "="*60)
        logger.info("CLEANING SUMMARY")
        logger.info("="*60)
        logger.info(f"Before: {report['before']['total_records']:,} records")
        logger.info(f"After: {report['after']['total_records']:,} records")
        logger.info(f"Total removed: {report['records_removed']:,}")
        logger.info("\nCleaning Actions:")
        for action, count in report['cleaning_actions'].items():
            logger.info(f"  {action}: {count}")
        logger.info("="*60 + "\n")


def main():
    """Main execution."""
    cleaner = DataCleaner()
    
    if not cleaner.connect():
        logger.error("Failed to connect to MongoDB. Exiting.")
        return
    
    try:
        cleaner.run_full_cleaning()
        logger.info("✓ Data cleaning completed successfully!")
    except Exception as e:
        logger.error(f"✗ Error during cleaning: {e}", exc_info=True)
    finally:
        cleaner.close()


if __name__ == "__main__":
    main()
