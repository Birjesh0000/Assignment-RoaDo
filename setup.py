#!/usr/bin/env python3
"""
Project Initialization & Setup Script
======================================
Automated setup for the complete RoaDo Analytics project.
Handles database setup, dependencies, and initial configuration.

Usage: python setup.py
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProjectSetup:
    """Setup and initialize the RoaDo Analytics project"""

    def __init__(self):
        self.project_root = Path(__file__).parent
        self.backend_dir = self.project_root / 'backend'
        self.frontend_dir = self.project_root / 'frontend'
        self.data_dir = self.project_root / 'data-scripts'

    def run_command(self, cmd: str, cwd: str = None, description: str = None) -> bool:
        """Run a shell command and handle errors"""
        try:
            if description:
                logger.info(f'→ {description}...')
            result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f'Command failed: {cmd}')
                if result.stderr:
                    logger.error(f'Error: {result.stderr}')
                return False
            return True
        except Exception as e:
            logger.error(f'Exception running command: {e}')
            return False

    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are installed"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Checking Prerequisites             ║')
        logger.info('╚═════════════════════════════════════╝\n')

        # Check Node.js
        if not self.run_command('node --version', description='Checking Node.js'):
            logger.error('Node.js is required but not installed')
            return False
        logger.info('✓ Node.js installed')

        # Check npm
        if not self.run_command('npm --version', description='Checking npm'):
            logger.error('npm is required but not installed')
            return False
        logger.info('✓ npm installed')

        # Check Python
        if not self.run_command('python --version', description='Checking Python'):
            logger.error('Python 3.8+ is required but not installed')
            return False
        logger.info('✓ Python installed')

        return True

    def install_python_dependencies(self) -> bool:
        """Install Python dependencies"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Installing Python Dependencies     ║')
        logger.info('╚═════════════════════════════════════╝\n')

        try:
            logger.info('→ Installing packages from requirements.txt...')
            result = subprocess.run(
                'pip install -r requirements.txt',
                shell=True,
                cwd=str(self.project_root),
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.error('Failed to install Python dependencies')
                logger.error(result.stderr)
                return False

            logger.info('✓ Python dependencies installed')
            return True
        except Exception as e:
            logger.error(f'Error installing Python dependencies: {e}')
            return False

    def install_backend_dependencies(self) -> bool:
        """Install Node.js dependencies for backend"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Installing Backend Dependencies    ║')
        logger.info('╚═════════════════════════════════════╝\n')

        return self.run_command(
            'npm install',
            cwd=str(self.backend_dir),
            description='Installing backend npm packages'
        ) and (logger.info('✓ Backend dependencies installed') or True)

    def install_frontend_dependencies(self) -> bool:
        """Install Node.js dependencies for frontend"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Installing Frontend Dependencies   ║')
        logger.info('╚═════════════════════════════════════╝\n')

        return self.run_command(
            'npm install',
            cwd=str(self.frontend_dir),
            description='Installing frontend npm packages'
        ) and (logger.info('✓ Frontend dependencies installed') or True)

    def verify_env_files(self) -> bool:
        """Verify and create .env files if needed"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Verifying Environment Files       ║')
        logger.info('╚═════════════════════════════════════╝\n')

        # Check backend .env
        backend_env = self.backend_dir / '.env'
        if not backend_env.exists():
            logger.warning('Backend .env not found, creating...')
            backend_env.write_text('''# Backend Server Configuration
NODE_ENV=development
PORT=5000

# MongoDB Connection (Local)
MONGODB_URI=mongodb://localhost:27017/nimbus_events

# PostgreSQL Connection (Local)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nimbus_core
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# CORS settings
CORS_ORIGIN=http://localhost:3000

# Logging
LOG_LEVEL=info
''')
            logger.info('✓ Created backend .env')
        else:
            logger.info('✓ Backend .env exists')

        # Check frontend .env
        frontend_env = self.frontend_dir / '.env'
        if not frontend_env.exists():
            logger.warning('Frontend .env not found, creating...')
            frontend_env.write_text('''REACT_APP_API_URL=http://localhost:5000/api/v1
REACT_APP_POSTGRES_API_URL=http://localhost:5000/api/v1/postgres
''')
            logger.info('✓ Created frontend .env')
        else:
            logger.info('✓ Frontend .env exists')

        return True

    def generate_sample_data(self) -> bool:
        """Generate sample data for testing"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Generating Sample Test Data       ║')
        logger.info('╚═════════════════════════════════════╝\n')

        return self.run_command(
            'python generate_sample_data.py',
            cwd=str(self.data_dir),
            description='Generating sample data'
        ) and (logger.info('✓ Sample data generated in data/sample/') or True)

    def verify_project_structure(self) -> bool:
        """Verify project structure is complete"""
        logger.info('\n╔═════════════════════════════════════╗')
        logger.info('║   Verifying Project Structure       ║')
        logger.info('╚═════════════════════════════════════╝\n')

        required_paths = [
            ('backend/src/config/database.js', 'MongoDB config'),
            ('backend/src/config/postgres.js', 'PostgreSQL config'),
            ('backend/src/services/analyticsService.js', 'Analytics service'),
            ('backend/src/services/postgresAnalyticsService.js', 'PostgreSQL service'),
            ('backend/src/routes/analytics.js', 'Analytics routes'),
            ('backend/src/routes/postgres.js', 'PostgreSQL routes'),
            ('frontend/src/components/Dashboard.js', 'React dashboard'),
            ('queries/mongodb-queries.js', 'MongoDB queries'),
            ('database/postgres/schema.sql', 'PostgreSQL schema'),
            ('database/postgres/postgres-queries.sql', 'PostgreSQL queries'),
            ('data-scripts/merge_and_analyze.py', 'Data analysis script'),
            ('data-scripts/load_data.py', 'Data loader'),
        ]

        all_exist = True
        for rel_path, description in required_paths:
            full_path = self.project_root / rel_path
            if full_path.exists():
                logger.info(f'✓ {description}: {rel_path}')
            else:
                logger.error(f'✗ MISSING: {description}: {rel_path}')
                all_exist = False

        return all_exist

    def print_next_steps(self):
        """Print next steps for user"""
        logger.info('\n╔══════════════════════════════════════════════════════════╗')
        logger.info('║              Setup Complete - Next Steps                ║')
        logger.info('╚══════════════════════════════════════════════════════════╝\n')

        logger.info('1. SET UP DATABASES:\n')
        logger.info('   PostgreSQL (nimbus_core):')
        logger.info('     • Create database: createdb nimbus_core')
        logger.info('     • Run schema: psql nimbus_core < database/postgres/schema.sql\n')

        logger.info('   MongoDB (nimbus_events):')
        logger.info('     • Start MongoDB: mongod')
        logger.info('     • Create collections in mongosh\n')

        logger.info('2. LOAD SAMPLE DATA:\n')
        logger.info('   python data-scripts/load_data.py data/sample/postgres data/sample/mongo\n')

        logger.info('3. START BACKEND & FRONTEND:\n')
        logger.info('   Terminal 1 - Backend:')
        logger.info('     cd backend && npm start\n')
        logger.info('   Terminal 2 - Frontend:')
        logger.info('     cd frontend && npm start\n')

        logger.info('   Terminal 3 - Analysis (optional):')
        logger.info('     python data-scripts/merge_and_analyze.py\n')

        logger.info('4. ACCESS SERVICES:\n')
        logger.info('   • React Dashboard: http://localhost:3000')
        logger.info('   • MongoDB APIs: http://localhost:5000/api/v1/analytics')
        logger.info('   • PostgreSQL APIs: http://localhost:5000/api/v1/postgres')
        logger.info('   • Health Check: http://localhost:5000/api/health\n')

        logger.info('5. YOUR CSV DATA:\n')
        logger.info('   When ready with CSV files:')
        logger.info('   python data-scripts/load_data.py <path_to_postgres_csv> <path_to_mongo_csv>\n')

    def run(self) -> bool:
        """Execute full setup"""
        logger.info('\n╔════════════════════════════════════════════════════════════╗')
        logger.info('║        RoaDo Analytics - Project Initialization             ║')
        logger.info('╚════════════════════════════════════════════════════════════╝\n')

        steps = [
            ('Prerequisites', self.check_prerequisites),
            ('Python Dependencies', self.install_python_dependencies),
            ('Backend Dependencies', self.install_backend_dependencies),
            ('Frontend Dependencies', self.install_frontend_dependencies),
            ('Environment Files', self.verify_env_files),
            ('Project Structure', self.verify_project_structure),
            ('Sample Data', self.generate_sample_data),
        ]

        for step_name, step_func in steps:
            try:
                if not step_func():
                    logger.error(f'✗ {step_name} failed')
                    return False
            except Exception as e:
                logger.error(f'✗ {step_name} error: {e}')
                return False

        self.print_next_steps()
        logger.info('═' * 60)
        logger.info('✓ Initial setup completed successfully!')
        logger.info('═' * 60 + '\n')
        return True


if __name__ == '__main__':
    setup = ProjectSetup()
    success = setup.run()
    sys.exit(0 if success else 1)
