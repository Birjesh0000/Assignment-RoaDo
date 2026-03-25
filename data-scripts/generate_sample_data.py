#!/usr/bin/env python3
"""
Sample Data Generator for Testing
==================================
Generates realistic sample data for both PostgreSQL and MongoDB
to enable testing without external CSV files.

Usage: python data-scripts/generate_sample_data.py
"""

import os
import json
import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SampleDataGenerator:
    """Generate realistic sample data for testing"""

    def __init__(self, output_dir: str = 'data/sample'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'postgres'), exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'mongo'), exist_ok=True)

    def generate_plans(self, count: int = 3) -> List[Dict]:
        """Generate subscription plans"""
        plans = [
            {
                'plan_id': 1,
                'plan_name': 'Starter',
                'tier': 'free',
                'base_price': '0.00',
                'features_included': 'Basic reporting, 3 users, 10GB storage'
            },
            {
                'plan_id': 2,
                'plan_name': 'Professional',
                'tier': 'pro',
                'base_price': '99.00',
                'features_included': 'Advanced analytics, 50 users, 500GB storage, API access'
            },
            {
                'plan_id': 3,
                'plan_name': 'Enterprise',
                'tier': 'enterprise',
                'base_price': '499.00',
                'features_included': 'Custom solutions, unlimited users, dedicated support, SSO'
            }
        ]
        return plans

    def generate_customers(self, count: int = 50) -> List[Dict]:
        """Generate customer records"""
        companies = [
            'TechCorp', 'DataFlow', 'CloudSync', 'InnovateLabs', 'DigitalFirst',
            'NextGen Solutions', 'Smart Analytics', 'Cloud Masters', 'Data Driven', 'Enterprise AI',
            'BlockChain Inc', 'DevOps Pro', 'Security Plus', 'Growth Labs', 'Marketing Pro'
        ]
        domains = ['com', 'io', 'co', 'org', 'net']

        customers = []
        for i in range(1, count + 1):
            company = random.choice(companies) + random.choice([' Inc', ' LLC', ' Corp', ''])
            domain = f"{company.lower().replace(' ', '')}.{random.choice(domains)}"
            email = f"contact@{domain}"

            customers.append({
                'customer_id': i,
                'company_name': company,
                'email': email,
                'email_domain': domain.split('@')[-1],
                'phone': f"+1{random.randint(2000000000, 9999999999)}",
                'country': random.choice(['USA', 'UK', 'Canada', 'Australia', 'Germany', 'France']),
                'created_at': (datetime.utcnow() - timedelta(days=random.randint(30, 365))).isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            })

        return customers

    def generate_subscriptions(self, customers: List[Dict], plans: List[Dict], count_per_customer: int = 2) -> List[Dict]:
        """Generate subscription records"""
        subscriptions = []
        sub_id = 1

        for customer in customers:
            # Most customers have 1-2 subscriptions
            num_subs = random.randint(1, count_per_customer)

            for _ in range(num_subs):
                plan = random.choice(plans)
                start_date = datetime.utcnow() - timedelta(days=random.randint(1, 365))
                status = random.choices(
                    ['active', 'cancelled', 'paused', 'downgraded'],
                    weights=[70, 15, 10, 5]
                )[0]
                end_date = None
                downgrade_date = None
                cancellation_date = None

                if status == 'cancelled':
                    end_date = (start_date + timedelta(days=random.randint(30, 350))).isoformat()
                    cancellation_date = end_date
                elif status == 'downgraded':
                    downgrade_date = (start_date + timedelta(days=random.randint(30, 300))).isoformat()
                    end_date = downgrade_date

                # Pricing based on tier
                tier_prices = {'free': 0, 'pro': 99, 'enterprise': 499}
                monthly_revenue = str(tier_prices[plan['tier']])

                subscriptions.append({
                    'subscription_id': sub_id,
                    'customer_id': customer['customer_id'],
                    'plan_id': plan['plan_id'],
                    'previous_plan_id': None if status != 'downgraded' else random.choice([1, 2]) if plan['plan_id'] != 1 else None,
                    'status': status,
                    'monthly_revenue': monthly_revenue,
                    'start_date': start_date.date().isoformat(),
                    'end_date': end_date,
                    'downgrade_date': downgrade_date,
                    'cancellation_date': cancellation_date,
                    'created_at': start_date.isoformat(),
                    'updated_at': datetime.utcnow().isoformat()
                })
                sub_id += 1

        return subscriptions

    def generate_billing(self, subscriptions: List[Dict]) -> List[Dict]:
        """Generate billing records"""
        billing = []
        billing_id = 1

        for sub in subscriptions:
            if sub['monthly_revenue'] == '0':
                continue

            # Generate 3-12 invoices per subscription
            num_invoices = random.randint(3, 12)
            start_date = datetime.fromisoformat(sub['start_date'])

            for i in range(num_invoices):
                invoice_date = start_date + timedelta(days=30 * i)

                # Stop at end date or today
                if sub['end_date']:
                    end_date = datetime.fromisoformat(sub['end_date'].split('T')[0])
                    if invoice_date >= end_date:
                        break
                elif invoice_date > datetime.utcnow():
                    break

                billing.append({
                    'billing_id': billing_id,
                    'subscription_id': sub['subscription_id'],
                    'customer_id': sub['customer_id'],
                    'invoice_date': invoice_date.date().isoformat(),
                    'amount': sub['monthly_revenue'],
                    'currency': 'USD',
                    'billing_period_start': invoice_date.date().isoformat(),
                    'billing_period_end': (invoice_date + timedelta(days=30)).date().isoformat(),
                    'payment_status': random.choice(['paid', 'pending', 'failed']),
                    'created_at': invoice_date.isoformat()
                })
                billing_id += 1

        return billing

    def generate_support_tickets(self, customers: List[Dict]) -> List[Dict]:
        """Generate support tickets"""
        tickets = []
        ticket_id = 1
        titles = [
            'API not responding',
            'Dashboard slow',
            'Integration issue',
            'Feature request',
            'Login problem',
            'Data export error',
            'Billing inquiry',
            'Permission denied',
            'Report not loading',
            'Email notification missing'
        ]
        priorities = ['low', 'medium', 'high', 'critical']

        for customer in customers:
            # Generate 0-5 tickets per customer in last 180 days
            num_tickets = random.randint(0, 5)

            for _ in range(num_tickets):
                created_at = datetime.utcnow() - timedelta(days=random.randint(0, 180))
                status = random.choices(['open', 'in_progress', 'resolved', 'closed'], weights=[20, 20, 40, 20])[0]
                resolved_at = None

                if status in ['resolved', 'closed']:
                    resolved_at = (created_at + timedelta(hours=random.randint(1, 72))).isoformat()

                tickets.append({
                    'ticket_id': ticket_id,
                    'customer_id': customer['customer_id'],
                    'subscription_id': random.randint(1, 30),
                    'title': random.choice(titles),
                    'description': f"Issue reported by customer on {created_at.date()}",
                    'priority': random.choice(priorities),
                    'status': status,
                    'created_at': created_at.isoformat(),
                    'resolved_at': resolved_at,
                    'updated_at': datetime.utcnow().isoformat()
                })
                ticket_id += 1

        return tickets

    def generate_team_members(self, customers: List[Dict]) -> List[Dict]:
        """Generate team members"""
        team_members = []
        member_id = 1
        first_names = ['John', 'Mary', 'James', 'Patricia', 'Michael', 'Jennifer', 'David', 'Linda']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        roles = ['Admin', 'Manager', 'Analyst', 'Developer', 'Viewer']

        for customer in customers:
            # Generate 1-5 team members per customer
            num_members = random.randint(1, 5)

            for _ in range(num_members):
                first = random.choice(first_names)
                last = random.choice(last_names)
                domain = customer['email_domain']

                team_members.append({
                    'team_member_id': member_id,
                    'customer_id': customer['customer_id'],
                    'name': f"{first} {last}",
                    'email': f"{first.lower()}.{last.lower()}@{domain}",
                    'email_domain': domain,
                    'role': random.choice(roles),
                    'invited_date': (datetime.utcnow() - timedelta(days=random.randint(1, 365))).isoformat(),
                    'created_at': datetime.utcnow().isoformat()
                })
                member_id += 1

        return team_members

    def generate_mongodb_events(self, customers: List[Dict]) -> List[Dict]:
        """Generate user activity events"""
        events = []
        event_types = ['session_start', 'session_end', 'feature_use', 'report_generated', 'export_completed', 'login', 'logout']
        features = ['dashboard', 'analytics', 'reporting', 'exporting', 'settings', 'api', 'integration']

        for customer in customers:
            # Generate 20-100 events per customer in last 180 days
            num_events = random.randint(20, 100)

            for _ in range(num_events):
                timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 180))

                events.append({
                    'customer_id': customer['customer_id'],
                    'user_id': f"user_{customer['customer_id']}_{random.randint(1, 5)}",
                    'event_type': random.choice(event_types),
                    'feature': random.choice(features),
                    'timestamp': timestamp.isoformat(),
                    'properties': {
                        'session_duration': random.randint(60, 3600),
                        'page_views': random.randint(1, 20),
                        'actions': random.randint(1, 50)
                    }
                })

        return events

    def generate_mongodb_sessions(self, customers: List[Dict]) -> List[Dict]:
        """Generate user sessions"""
        sessions = []

        for customer in customers:
            # Generate 10-50 sessions per customer in last 180 days
            num_sessions = random.randint(10, 50)

            for i in range(num_sessions):
                start_time = datetime.utcnow() - timedelta(days=random.randint(0, 180))
                duration = random.randint(300, 7200)

                sessions.append({
                    'session_id': f"session_{customer['customer_id']}_{i}",
                    'customer_id': customer['customer_id'],
                    'user_id': f"user_{customer['customer_id']}_{random.randint(1, 5)}",
                    'start_time': start_time.isoformat(),
                    'end_time': (start_time + timedelta(seconds=duration)).isoformat(),
                    'duration': duration,
                    'device': random.choice(['desktop', 'mobile', 'tablet']),
                    'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                    'ip_address': '.'.join(str(random.randint(0, 255)) for _ in range(4))
                })

        return sessions

    def save_csv(self, data: List[Dict], filename: str, directory: str):
        """Save data to CSV file"""
        if not data:
            logger.warning(f"No data to save for {filename}")
            return

        filepath = os.path.join(self.output_dir, directory, filename)
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            logger.info(f"✓ Generated {len(data)} records in {filename}")
        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")

    def run(self):
        """Generate all sample data"""
        logger.info('\n╔══════════════════════════════════════╗')
        logger.info('║   Sample Data Generator for Testing  ║')
        logger.info('╚══════════════════════════════════════╝\n')

        # Generate PostgreSQL data
        logger.info('Generating PostgreSQL data...\n')

        plans = self.generate_plans()
        self.save_csv(plans, 'plans.csv', 'postgres')

        customers = self.generate_customers(50)
        self.save_csv(customers, 'customers.csv', 'postgres')

        subscriptions = self.generate_subscriptions(customers, plans)
        self.save_csv(subscriptions, 'subscriptions.csv', 'postgres')

        billing = self.generate_billing(subscriptions)
        self.save_csv(billing, 'billing.csv', 'postgres')

        tickets = self.generate_support_tickets(customers)
        self.save_csv(tickets, 'support_tickets.csv', 'postgres')

        team_members = self.generate_team_members(customers)
        self.save_csv(team_members, 'team_members.csv', 'postgres')

        # Feature flags
        feature_flags = [
            {
                'flag_id': i,
                'customer_id': random.choice(customers)['customer_id'],
                'subscription_id': random.randint(1, len(subscriptions)),
                'feature_name': random.choice(['advanced_analytics', 'api_access', 'sso', 'custom_branding']),
                'is_enabled': random.choice([True, False]),
                'enabled_date': (datetime.utcnow() - timedelta(days=random.randint(1, 180))).isoformat(),
                'created_at': datetime.utcnow().isoformat()
            }
            for i in range(1, 100)
        ]
        self.save_csv(feature_flags, 'feature_flags.csv', 'postgres')

        logger.info('\nGenerating MongoDB data...\n')

        events = self.generate_mongodb_events(customers)
        self.save_csv(events, 'events.csv', 'mongo')

        sessions = self.generate_mongodb_sessions(customers)
        self.save_csv(sessions, 'sessions.csv', 'mongo')

        logger.info(f'\n✓ Sample data generated in: {os.path.abspath(self.output_dir)}\n')
        logger.info('Usage:\n')
        logger.info(f'  python data-scripts/load_data.py {os.path.join(self.output_dir, "postgres")} {os.path.join(self.output_dir, "mongo")}\n')


if __name__ == '__main__':
    generator = SampleDataGenerator()
    generator.run()
