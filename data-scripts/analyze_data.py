#!/usr/bin/env python3
"""
Data Analysis & Quality Assessment Script
==========================================

This script connects to MongoDB nimbus_events collection, analyzes data quality,
identifies issues, and generates a comprehensive QA report.

Author: Data Analyst
Date: 2026-03-25
"""

import os
import json
import logging
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Any

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
except ImportError:
    print("ERROR: pymongo not installed. Run: pip install pymongo")
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data-scripts/analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataAnalyzer:
    """Analyzes MongoDB nimbus_events collection for data quality issues."""

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
        self.quality_report = {}
        
    def connect(self) -> bool:
        """Establish MongoDB connection."""
        try:
            logger.info(f"Connecting to MongoDB: {self.mongo_uri}")
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.collection = self.client[self.db_name][self.collection_name]
            self.total_records = self.collection.count_documents({})
            logger.info(f"✓ Connected successfully. Found {self.total_records:,} records")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"✗ Connection failed: {e}")
            return False

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def sample_documents(self, limit: int = 10) -> List[Dict]:
        """Get sample documents from collection."""
        return list(self.collection.find().limit(limit))

    def analyze_missing_values(self) -> Dict[str, float]:
        """Analyze missing/null values in each field."""
        logger.info("Analyzing missing values...")
        
        required_fields = ['user_id', 'timestamp', 'event_type', 'subscription_tier']
        missing_analysis = {}
        
        for field in required_fields:
            missing_count = self.collection.count_documents({field: None}) + \
                           self.collection.count_documents({field: {'$exists': False}})
            missing_pct = (missing_count / self.total_records) * 100
            missing_analysis[field] = {
                'count': missing_count,
                'percentage': round(missing_pct, 2)
            }
            logger.info(f"  {field}: {missing_count:,} missing ({missing_pct:.2f}%)")
        
        return missing_analysis

    def analyze_duplicates(self) -> Dict[str, Any]:
        """Detect exact and near-duplicate records."""
        logger.info("Analyzing duplicates...")
        
        # Exact duplicates: same user_id + event_type + timestamp (within 1 second)
        duplicate_query = [
            {
                '$group': {
                    '_id': {
                        'user_id': '$user_id',
                        'event_type': '$event_type',
                        'timestamp': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$timestamp'}}
                    },
                    'count': {'$sum': 1},
                    'ids': {'$push': '$_id'}
                }
            },
            {
                '$match': {'count': {'$gt': 1}}
            },
            {
                '$limit': 1000  # Sample of duplicates
            }
        ]
        
        duplicates = list(self.collection.aggregate(duplicate_query))
        logger.info(f"  Found {len(duplicates)} duplicate patterns (sample)")
        
        return {
            'duplicate_patterns_found': len(duplicates),
            'sample_duplicates': duplicates[:5]
        }

    def analyze_timestamps(self) -> Dict[str, Any]:
        """Analyze timestamp distribution and timezone info."""
        logger.info("Analyzing timestamps...")
        
        # Get date range
        agg_query = [
            {
                '$group': {
                    '_id': None,
                    'min_date': {'$min': '$timestamp'},
                    'max_date': {'$max': '$timestamp'},
                    'total_events': {'$sum': 1}
                }
            }
        ]
        
        result = list(self.collection.aggregate(agg_query))[0]
        min_date = result['min_date']
        max_date = result['max_date']
        date_range = (max_date - min_date).days
        
        logger.info(f"  Date range: {min_date} to {max_date} ({date_range} days)")
        
        return {
            'min_date': min_date.isoformat(),
            'max_date': max_date.isoformat(),
            'date_range_days': date_range,
            'total_events': result['total_events']
        }

    def analyze_event_types(self) -> Dict[str, int]:
        """Analyze distribution of event types."""
        logger.info("Analyzing event types...")
        
        event_type_query = [
            {
                '$group': {
                    '_id': '$event_type',
                    'count': {'$sum': 1},
                    'pct': {'$sum': 100}
                }
            },
            {'$sort': {'count': -1}},
            {'$limit': 50}
        ]
        
        results = list(self.collection.aggregate(event_type_query))
        event_distribution = {item['_id']: item['count'] for item in results}
        
        logger.info(f"  Found {len(event_distribution)} unique event types")
        for event_type, count in list(event_distribution.items())[:10]:
            pct = (count / self.total_records) * 100
            logger.info(f"    {event_type}: {count:,} ({pct:.2f}%)")
        
        return event_distribution

    def analyze_tiers(self) -> Dict[str, int]:
        """Analyze subscription tier distribution."""
        logger.info("Analyzing subscription tiers...")
        
        tier_query = [
            {
                '$group': {
                    '_id': '$subscription_tier',
                    'count': {'$sum': 1}
                }
            },
            {'$sort': {'count': -1}}
        ]
        
        results = list(self.collection.aggregate(tier_query))
        tier_distribution = {item['_id']: item['count'] for item in results}
        
        for tier, count in tier_distribution.items():
            pct = (count / self.total_records) * 100
            logger.info(f"  {tier}: {count:,} ({pct:.2f}%)")
        
        return tier_distribution

    def analyze_NULL_values(self) -> Dict[str, Any]:
        """Analyze NULL/None values in data."""
        logger.info("Analyzing NULL values...")
        
        sample_docs = self.sample_documents(100)
        null_fields = defaultdict(int)
        
        for doc in sample_docs:
            for key, value in doc.items():
                if value is None or value == '' or value == 'null':
                    null_fields[key] += 1
        
        logger.info(f"  Found {len(null_fields)} fields with NULL values (in sample)")
        for field, count in sorted(null_fields.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    {field}: {count}% of sample")
        
        return dict(null_fields)

    def analyze_outliers(self) -> Dict[str, Any]:
        """Identify potential outliers in numeric fields."""
        logger.info("Analyzing outliers...")
        
        # Check for extreme duration values if they exist
        outlier_query = [
            {
                '$group': {
                    '_id': None,
                    'avg_duration': {'$avg': '$properties.duration_seconds'},
                    'max_duration': {'$max': '$properties.duration_seconds'},
                    'min_duration': {'$min': '$properties.duration_seconds'}
                }
            }
        ]
        
        try:
            result = list(self.collection.aggregate(outlier_query))[0]
            logger.info(f"  Duration stats - Min: {result['min_duration']}, Max: {result['max_duration']}, Avg: {result['avg_duration']:.2f}")
            return result
        except (KeyError, IndexError) as e:
            logger.warning(f"  Could not analyze duration outliers: {e}")
            return {}

    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report."""
        logger.info("\n" + "="*60)
        logger.info("GENERATING DATA QUALITY REPORT")
        logger.info("="*60 + "\n")
        
        self.quality_report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': self.total_records,
            'missing_values': self.analyze_missing_values(),
            'duplicates': self.analyze_duplicates(),
            'timestamps': self.analyze_timestamps(),
            'event_types': self.analyze_event_types(),
            'subscription_tiers': self.analyze_tiers(),
            'null_values_sample': self.analyze_NULL_values(),
            'outliers': self.analyze_outliers(),
            'sample_documents': [
                {
                    '_id': str(doc.get('_id')),
                    'user_id': doc.get('user_id'),
                    'event_type': doc.get('event_type'),
                    'timestamp': doc.get('timestamp').isoformat() if doc.get('timestamp') else None,
                    'subscription_tier': doc.get('subscription_tier')
                }
                for doc in self.sample_documents(5)
            ]
        }
        
        return self.quality_report

    def save_report(self, output_path: str = 'data-scripts/data_quality_report.json'):
        """Save quality report to JSON file."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.quality_report, f, indent=2, default=str)
        logger.info(f"\n✓ Report saved to {output_path}")

    def print_summary(self):
        """Print quality summary to console."""
        logger.info("\n" + "="*60)
        logger.info("DATA QUALITY SUMMARY")
        logger.info("="*60)
        
        if not self.quality_report:
            logger.warning("No report generated yet. Run generate_report() first.")
            return
        
        logger.info(f"Total Records: {self.quality_report['total_records']:,}")
        logger.info(f"Analysis Date: {self.quality_report['timestamp']}")
        
        logger.info("\nMissing Values:")
        for field, stats in self.quality_report['missing_values'].items():
            logger.info(f"  {field}: {stats['percentage']:.2f}% missing")
        
        logger.info(f"\nDuplicate Patterns: {self.quality_report['duplicates']['duplicate_patterns_found']}")
        
        logger.info("\nDate Range:")
        ts = self.quality_report['timestamps']
        logger.info(f"  From: {ts['min_date'][:10]}")
        logger.info(f"  To: {ts['max_date'][:10]}")
        logger.info(f"  Range: {ts['date_range_days']} days")
        
        logger.info(f"\nEvent Types: {len(self.quality_report['event_types'])} unique types")
        logger.info(f"Subscription Tiers: {len(self.quality_report['subscription_tiers'])} types")
        
        logger.info("\n" + "="*60 + "\n")


def main():
    """Main execution."""
    analyzer = DataAnalyzer()
    
    if not analyzer.connect():
        logger.error("Failed to connect to MongoDB. Exiting.")
        return
    
    try:
        # Generate report
        analyzer.generate_report()
        analyzer.print_summary()
        analyzer.save_report()
        
        # Also save sample documents
        samples = analyzer.sample_documents(20)
        sample_path = 'data-scripts/sample_documents.json'
        os.makedirs(os.path.dirname(sample_path), exist_ok=True)
        with open(sample_path, 'w') as f:
            json.dump(
                [{'_id': str(doc.get('_id')), **{k: v for k, v in doc.items() if k != '_id'}} 
                 for doc in samples],
                f,
                indent=2,
                default=str
            )
        logger.info(f"✓ Sample documents saved to {sample_path}")
        
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
