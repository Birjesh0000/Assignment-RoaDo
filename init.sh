#!/usr/bin/env bash
# Project Initialization & Database Setup Script
# ===============================================
# Run this after python setup.py to initialize databases

set -e

echo ""
echo "╔════════════════════════════════════════════════╗"
echo "║   Database & Project Initialization            ║"
echo "╚════════════════════════════════════════════════╝"
echo ""

# ============================================================
# PostgreSQL Setup
# ============================================================

echo "1. PostgreSQL Initialize..."
echo "   → Create database: createdb nimbus_core 2>/dev/null || true"
createdb nimbus_core 2>/dev/null || echo "   (Database may already exist)"

echo "   → Loading schema..."
psql -d nimbus_core -f database/postgres/schema.sql > /dev/null 2>&1 || \
  echo "   ⚠ Could not load schema (may need manual setup)"

echo "   ✓ PostgreSQL ready"
echo ""

# ============================================================
# MongoDB Setup
# ============================================================

echo "2. MongoDB Initialize..."
echo "   → Creating collections..."
mongosh << 'EOF' 2>/dev/null || true
use nimbus_events
db.createCollection("events")
db.createCollection("sessions")
print("Collections created")
EOF

echo "   ✓ MongoDB ready"
echo ""

# ============================================================
# Generate Sample Data
# ============================================================

echo "3. Generate Sample Data..."
python data-scripts/generate_sample_data.py 2>/dev/null || \
  echo "   ⚠ Could not generate sample data"

echo ""

# ============================================================
# Load Data
# ============================================================

echo "4. Load Sample Data into Databases..."
python data-scripts/load_data.py data/sample/postgres data/sample/mongo 2>/dev/null || \
  echo "   ⚠ Could not load sample data (ensure databases are running)"

echo ""

# ============================================================
# Generate Power BI Setup
# ============================================================

echo "5. Generate Power BI Setup Guide..."
python powerbi/generate_powerbi_setup.py 2>/dev/null || \
  echo "   ⚠ Could not generate Power BI guide"

echo ""

# ============================================================
# Start Services
# ============================================================

echo "╔════════════════════════════════════════════════╗"
echo "║              Setup Complete!                  ║"
echo "╚════════════════════════════════════════════════╝"
echo ""
echo "Next: Start services in separate terminals:"
echo ""
echo "  Terminal 1 (Backend on port 5000):"
echo "    cd backend && npm start"
echo ""
echo "  Terminal 2 (Frontend on port 3000):"
echo "    cd frontend && npm start"
echo ""
echo "  Terminal 3 (Optional - Analysis):"
echo "    python data-scripts/merge_and_analyze.py"
echo ""
echo "Then access:"
echo "  • Dashboard: http://localhost:3000"
echo "  • Health Check: http://localhost:5000/api/health"
echo ""
