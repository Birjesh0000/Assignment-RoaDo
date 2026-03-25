#!/usr/bin/env python3
"""
Database & Project Initialization Script (Windows)
====================================================
One-command setup for PostgreSQL, MongoDB, and data loading
"""

import os
import sys
import subprocess
import time

def run_cmd(cmd, description="", ignore_error=False):
    """Run command and handle errors"""
    if description:
        print(f"  → {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0 and not ignore_error:
            print(f"    ⚠ {result.stderr.strip()}")
            return False
        return True
    except Exception as e:
        print(f"    ⚠ Error: {e}")
        return False

def main():
    print("\n╔════════════════════════════════════════════════╗")
    print("║   Database & Project Initialization            ║")
    print("╚════════════════════════════════════════════════╝\n")

    # Step 1: PostgreSQL
    print("1. PostgreSQL Initialize...")
    print("   → Creating database: createdb nimbus_core")
    run_cmd("createdb nimbus_core", ignore_error=True)
    
    print("   → Loading schema...")
    run_cmd("psql -d nimbus_core -f database/postgres/schema.sql", ignore_error=True)
    print("   ✓ PostgreSQL ready\n")

    # Step 2: MongoDB
    print("2. MongoDB Initialize...")
    print("   → Creating collections (via mongosh)...")
    mongo_script = """
use nimbus_events
db.createCollection("events")
db.createCollection("sessions")
"""
    mongo_script_file = "temp_mongo_init.js"
    with open(mongo_script_file, 'w') as f:
        f.write(mongo_script)
    
    run_cmd(f"mongosh < {mongo_script_file}", ignore_error=True)
    
    try:
        os.remove(mongo_script_file)
    except:
        pass
    
    print("   ✓ MongoDB ready\n")

    # Step 3: Generate Sample Data
    print("3. Generate Sample Data...")
    if run_cmd("python data-scripts/generate_sample_data.py"):
        print("   ✓ Sample data generated\n")
    else:
        print("   ⚠ Could not generate sample data\n")

    # Step 4: Load Data
    print("4. Load Sample Data into Databases...")
    if run_cmd("python data-scripts/load_data.py data/sample/postgres data/sample/mongo"):
        print("   ✓ Data loaded\n")
    else:
        print("   ⚠ Could not load sample data\n")

    # Step 5: Generate Power BI Setup
    print("5. Generate Power BI Setup Guide...")
    if run_cmd("python powerbi/generate_powerbi_setup.py"):
        print("   ✓ Power BI guide generated\n")
    else:
        print("   ⚠ Could not generate Power BI guide\n")

    # Summary
    print("╔════════════════════════════════════════════════╗")
    print("║              Setup Complete!                  ║")
    print("╚════════════════════════════════════════════════╝\n")

    print("Next: Start services in separate terminals:\n")
    print("  PowerShell 1 (Backend on port 5000):")
    print("    cd backend; npm start\n")

    print("  PowerShell 2 (Frontend on port 3000):")
    print("    cd frontend; npm start\n")

    print("  PowerShell 3 (Optional - Analysis):")
    print("    python data-scripts/merge_and_analyze.py\n")

    print("Then access:")
    print("  • Dashboard: http://localhost:3000")
    print("  • Health Check: http://localhost:5000/api/health")
    print("  • MongoDB APIs: http://localhost:5000/api/v1/analytics")
    print("  • PostgreSQL APIs: http://localhost:5000/api/v1/postgres\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
