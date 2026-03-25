#!/usr/bin/env python3
"""
Phase 2: Data Wrangling & Validation Runner
============================================

Orchestrates data analysis and cleaning for the project.
This is the main entry point for Phase 2.

Usage:
    python run_phase2.py [analyze|clean|full]

    - analyze: Run data quality analysis only
    - clean: Run data cleaning only
    - full: Run both analysis and cleaning (default)
"""

import sys
import os
import logging
from datetime import datetime

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'data-scripts'))

try:
    from analyze_data import DataAnalyzer
    from clean_data import DataCleaner
except ImportError as e:
    print(f"ERROR: Could not import modules: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_analysis():
    """Run data quality analysis."""
    logger.info("\n" + "="*70)
    logger.info("PHASE 2.1: DATA QUALITY ANALYSIS")
    logger.info("="*70 + "\n")
    
    analyzer = DataAnalyzer()
    
    if not analyzer.connect():
        logger.error("Failed to connect to MongoDB for analysis")
        return False
    
    try:
        analyzer.generate_report()
        analyzer.print_summary()
        analyzer.save_report()
        logger.info("\n✓ Analysis completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Analysis failed: {e}", exc_info=True)
        return False
    finally:
        analyzer.close()


def run_cleaning():
    """Run data cleaning."""
    logger.info("\n" + "="*70)
    logger.info("PHASE 2.2: DATA CLEANING & VALIDATION")
    logger.info("="*70 + "\n")
    
    cleaner = DataCleaner()
    
    if not cleaner.connect():
        logger.error("Failed to connect to MongoDB for cleaning")
        return False
    
    try:
        cleaner.run_full_cleaning()
        logger.info("\n✓ Cleaning completed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Cleaning failed: {e}", exc_info=True)
        return False
    finally:
        cleaner.close()


def main():
    """Main execution."""
    mode = 'full'
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    
    if mode not in ['analyze', 'clean', 'full']:
        print(f"Invalid mode: {mode}")
        print("Usage: python run_phase2.py [analyze|clean|full]")
        sys.exit(1)
    
    logger.info("\n" + "="*70)
    logger.info("PHASE 2: DATA WRANGLING & CLEANING")
    logger.info(f"Mode: {mode.upper()}")
    logger.info(f"Started: {datetime.now()}")
    logger.info("="*70)
    
    success = True
    
    # Run analysis
    if mode in ['analyze', 'full']:
        if not run_analysis():
            success = False
            if mode == 'analyze':
                sys.exit(1)
    
    # Run cleaning
    if mode in ['clean', 'full']:
        if not run_cleaning():
            success = False
    
    # Summary
    logger.info("\n" + "="*70)
    if success:
        logger.info("✓ PHASE 2 COMPLETED SUCCESSFULLY")
    else:
        logger.info("✗ PHASE 2 COMPLETED WITH ERRORS")
    logger.info(f"Finished: {datetime.now()}")
    logger.info("="*70 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
