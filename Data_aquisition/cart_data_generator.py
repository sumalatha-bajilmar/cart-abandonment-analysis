import os
import json
import boto3
import uuid
import logging
import random
from datetime import datetime, timedelta
from faker import Faker

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

fake = Faker()

class ECommerceDataPipelineGenerator:
    """Generates separate but linked Customer Master and Cart Activity datasets"""
    
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.activity_types = {
            'login': 0.07,
            'logout': 0.05,
            'cart-add': 0.36,
            'cart-update': 0.25,
            'cart-remove': 0.15,
            'checkout': 0.12
        }
        self.categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Beauty', 'Sports']
        self.devices = ['Desktop', 'Mobile', 'Tablet']
        self.regions = ['US', 'EU', 'APAC', 'LATAM']
        
        # Define master database sizes
        self.num_master_customers = 100  # Total size of Master Customer Pool
        self.num_active_customers = 30   # Only a subset (30%) will generate cart activity logs
        
        # Storage for pools
        self.customer_master_pool = []
        self.active_customer_subset = []
        self.product_catalog = []
        self.active_sessions = {} 

        # Build catalogs upfront
        self._initialize_customer_master_pool()
        self._initialize_product_catalog()

    def _initialize_customer_master_pool(self):
        """Generates the Master Customer pool with stable, linked metadata profiles."""
        for _ in range(self.num_master_customers):
            user_id = str(uuid.uuid4())
            profile = fake.profile()
            self.customer_master_pool.append({
                'user_id': user_id,
                'name': profile['name'],
                'user_email': profile['mail'],
                'phone_number': fake.phone_number(),
                'address': profile['address'].replace('\n', ', '),
                'device_type': random.choice(self.devices),
                'region': random.choice(self.regions),
                'ip_address': fake.ipv4()
            })
        
        # Take a strict subset of these users to use for generating transactional logs
        self.active_customer_subset = random.sample(self.customer_master_pool, self.num_active_customers)

    def _initialize_product_catalog(self):
        """Pre-generates a catalog of items so update/remove actions target real items."""
        for _ in range(30):
            self.product_catalog.append({
                'product_id': str(uuid.uuid4()),
                'product_name': f"{fake.word().title()} {fake.word().title()}",
                'category': random.choice(self.categories),
                'price_per_unit': round(random.uniform(10, 500), 2)
            })

    def get_status_for_activity(self, activity_type):
        if activity_type == 'login':
            return random.choice(['success'] * 98 + ['failed'] * 2)
        elif activity_type == 'logout':
            return 'success'
        elif activity_type in ['cart-add', 'cart-update', 'cart-remove']:
            return random.choice(['success'] * 99 + ['failed'] * 1)
        elif activity_type == 'checkout':
            return random.choice(['success'] * 50 + ['failed'] * 15 + ['abandoned'] * 35)
        return 'success'

    def generate_activity_record(self, record_timestamp):
        """Generates a single cart transaction using only the active user subset."""
        activity_type = random.choices(
            list(self.activity_types.keys()),
            weights=list(self.activity_types.values())
        )[0]
        
        status = self.get_status_for_activity(activity_type)
        
        # 1. Pull user strictly from the active subset (Guarantees they exist in master)
        user = random.choice(self.active_customer_subset)
        user_id = user['user_id']
        
        if user_id not in self.active_sessions or activity_type == 'login':
            self.active_sessions[user_id] = str(uuid.uuid4())
        
        session_id = self.active_sessions[user_id]
        
        if activity_type in ['logout'] or (activity_type == 'checkout' and status == 'success'):
            self.active_sessions.pop(user_id, None)

        # 2. Map basic log metadata (Notice: we exclude profile data like email/address here)
        record = {
            'timestamp': record_timestamp.isoformat(),
            'user_id': user_id,
            'session_id': session_id,
            'activity_type': activity_type,
            'status': status
        }
        
        product = random.choice(self.product_catalog)

        if activity_type == 'login':
            record['login_method'] = random.choice(['email', 'google', 'facebook', 'apple'])
            if status == 'failed':
                record['failure_reason'] = random.choice(['invalid_credentials', 'account_locked', 'mfa_failed'])
        
        elif activity_type == 'logout':
            record['logout_reason'] = random.choice(['user_initiated', 'session_timeout'])
        
        elif activity_type in ['cart-add', 'cart-update', 'cart-remove']:
            record['product_id'] = product['product_id']
            record['product_name'] = product['product_name']
            record['category'] = product['category']
            record['quantity'] = random.randint(0, 5) if activity_type == 'cart-remove' else random.randint(1, 5)
            record['cart_price'] = product['price_per_unit']
            if status == 'failed':
                record['failure_reason'] = random.choice(['out_of_stock', 'invalid_quantity', 'product_not_found'])
        
        elif activity_type == 'checkout':
            record['order_id'] = str(uuid.uuid4()) if status == 'success' else None
            record['cart_total_amount'] = round(random.uniform(20, 500), 2)
            record['item_count'] = random.randint(1, 10)
            record['payment_method'] = random.choice(['credit_card', 'paypal', 'apple_pay'])
            if status == 'failed':
                record['failure_reason'] = random.choice(['card_declined', 'technical_error', 'inventory_issue'])
        
        return record
    
    def generate_pipeline_datasets(self):
        """Generates both datasets concurrently."""
        # Dataset 1: Customer Master
        df_master = self.customer_master_pool
        
        # Dataset 2: Sequential Cart Logs
        cart_logs = []
        current_time = datetime.utcnow() - timedelta(hours=6)
        for _ in range(self.batch_size):
            current_time += timedelta(seconds=random.randint(1, 15))
            cart_logs.append(self.generate_activity_record(current_time))
            
        return df_master, cart_logs


def lambda_handler(event, context):
    try:
        cart_bucket = 'shopin-cart-analysis'
        customer_bucket = 'shopin-customer-data'
        batch_size = 1000
        filedate = datetime.utcnow().strftime('%Y%m%d')
        batch_id = str(uuid.uuid4())[:8]
        
        # Initialize pipeline engine
        pipeline = ECommerceDataPipelineGenerator(batch_size)
        customer_master, cart_logs = pipeline.generate_pipeline_datasets()
        
        # --- PATH 1: Upload Customer Master Dimension (Saved as clean JSON Array) ---
        master_key = f"prod/customer_master_{batch_id}.json"
        s3.put_object(
            Bucket=customer_bucket,
            Key=master_key,
            Body=json.dumps(customer_master, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Uploaded customer master profile database to s3://{customer_bucket}/{master_key}")
        
        # --- PATH 2: Upload Cart Activity Log Partition (Saved as JSON Lines) ---
        logs_key = f"raw/{filedate}/cart_data_{batch_id}.json"
        logs_body = '\n'.join([json.dumps(log) for log in cart_logs])
        s3.put_object(
            Bucket=cart_bucket,
            Key=logs_key,
            Body=logs_body,
            ContentType='application/json'
        )
        logger.info(f"Uploaded transactional cart logs to s3://{cart_bucket}/{logs_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Pipeline simulation files split successfully',
                'customer_master_path': f"s3://{customer_bucket}/{master_key}",
                'cart_log_path': f"s3://{cart_bucket}/{logs_key}",
                'master_count': len(customer_master),
                'active_log_count': len(cart_logs)
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
