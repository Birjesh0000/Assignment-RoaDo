#!/bin/bash

# RoaDo Analytics Platform - Complete Setup & Run Guide
# =====================================================

echo "=========================================="
echo "RoaDo Analytics Platform - Setup Guide"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${GREEN}➤ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Step 1: MongoDB Setup
print_step "Step 1: MongoDB Connection"
echo "Required: MongoDB instance running"
echo "  - Local: mongodb://localhost:27017/nimbus_events"
echo "  - Cloud: MongoDB Atlas connection string"
echo "Make sure your .env files have correct MONGODB_URI"
echo ""

# Step 2: Backend Setup
print_step "Step 2: Backend Setup"
echo "cd backend"
echo "npm install"
echo ""
print_info "Configure backend/.env with:"
echo "  - MONGODB_URI=<your-mongo-connection>"
echo "  - PORT=5000"
echo "  - NODE_ENV=development"
echo ""

# Step 3: Frontend Setup
print_step "Step 3: Frontend Setup"
echo "cd frontend"
echo "npm install"
echo ""
print_info "Configure frontend/.env with:"
echo "  - REACT_APP_API_URL=http://localhost:5000/api/v1"
echo ""

# Step 4: Python Scripts
print_step "Step 4: Data Wrangling Scripts"
echo "pip install -r requirements.txt"
echo ""
print_info "Optional - Run data analysis:"
echo "  python run_phase2.py analyze    # Analyze data quality"
echo "  python run_phase2.py clean      # Clean and validate"
echo "  python run_phase2.py full       # Both"
echo ""

# Step 5: Running the Application
print_step "Step 5: Running the Full Stack"
echo ""
print_info "Terminal 1 - Start Backend API:"
echo "  cd backend"
echo "  npm start"
echo "  # Server will run on http://localhost:5000"
echo ""
print_info "Terminal 2 - Start Frontend:"
echo "  cd frontend"
echo "  npm start"
echo "  # Dashboard will open at http://localhost:3000"
echo ""
print_info "Terminal 3 (Optional) - Run Queries:"
echo "  node queries/run-queries.js    # Execute all queries"
echo "  node queries/run-queries.js 1  # Execute only Q1"
echo ""

# Step 6: Testing the API
print_step "Step 6: Test API Endpoints"
echo "Once backend is running, test endpoints via curl or browser:"
echo ""
echo "  # Health check"
echo "  curl http://localhost:5000/api/health"
echo ""
echo "  # Sessions by tier"
echo "  curl http://localhost:5000/api/v1/analytics/sessions-by-tier"
echo ""
echo "  # Feature DAU & retention"
echo "  curl http://localhost:5000/api/v1/analytics/feature-dau-retention"
echo ""
echo "  # Onboarding funnel"
echo "  curl http://localhost:5000/api/v1/analytics/onboarding-funnel"
echo ""
echo "  # Engaged free users"
echo "  curl http://localhost:5000/api/v1/analytics/engaged-free-users"
echo ""

# Project Structure
print_step "Step 7: Project Structure"
echo ""
echo "Assignment-RoaDo/"
echo "├── backend/                 # Express.js API"
echo "│   ├── server.js"
echo "│   ├── package.json"
echo "│   ├── .env"
echo "│   └── src/"
echo "│       ├── config/database.js"
echo "│       ├── controllers/"
echo "│       ├── services/"
echo "│       └── routes/"
echo "├── frontend/                # React.js Dashboard"
echo "│   ├── package.json"
echo "│   ├── .env"
echo "│   ├── public/"
echo "│   └── src/"
echo "│       ├── components/"
echo "│       ├── hooks/"
echo "│       └── services/"
echo "├── queries/                 # MongoDB Queries"
echo "│   ├── mongodb-queries.js"
echo "│   └── run-queries.js"
echo "├── data-scripts/            # Data Processing"
echo "│   ├── analyze_data.py"
echo "│   └── clean_data.py"
echo "├── PowerBI/                 # Power BI Files (manual)"
echo "└── requirements.txt         # Python dependencies"
echo ""

# Troubleshooting
print_step "Step 8: Troubleshooting"
echo ""
print_info "MongoDB Connection Issues:"
echo "  - Verify MongoDB is running"
echo "  - Check connection string in .env"
echo "  - For Atlas, ensure IP whitelist allows your connection"
echo ""
print_info "API Errors:"
echo "  - Check backend logs for detailed errors"
echo "  - Verify MongoDB collection name is 'events'"
echo ""
print_info "Frontend Not Connecting:"
echo "  - Ensure backend is running first (port 5000)"
echo "  - Check REACT_APP_API_URL in frontend/.env"
echo "  - Clear browser cache if needed"
echo ""
print_info "Port Already in Use:"
echo "  - Backend: Change PORT in backend/.env"
echo "  - Frontend: Set PORT=3001 npm start"
echo ""

# MongoDB Query Testing
print_step "Step 9: Execute MongoDB Queries"
echo ""
echo "From project root:"
echo "  node queries/run-queries.js     # Runs all 4 queries"
echo "  node queries/run-queries.js 1   # Runs only Q1 (Sessions by Tier)"
echo ""
print_info "Results saved to:"
echo "  - queries/query_1_results.json  # Q1: Sessions by Tier"
echo "  - queries/query_2_results.json  # Q2: Feature DAU & Retention"
echo "  - queries/query_3_results.json  # Q3: Onboarding Funnel"
echo "  - queries/query_4_results.json  # Q4: Engaged Free Users"
echo ""

# Power BI Setup
print_step "Step 10: Power BI Dashboard"
echo ""
echo "Manual Setup Required:"
echo "  1. Open Power BI Desktop"
echo "  2. New Report → Get Data → MongoDB (or Web/URL)"
echo "  3. Connect to backend API: http://localhost:5000/api/v1/analytics"
echo "  4. Import 4 queries as data sources"
echo "  5. Create visualizations (5+ required)"
echo "  6. Add datetime filters"
echo "  7. Save as .pbix file"
echo ""

echo ""
print_step "Setup Complete!"
echo "Ready to run the RoaDo Analytics Platform"
echo ""
