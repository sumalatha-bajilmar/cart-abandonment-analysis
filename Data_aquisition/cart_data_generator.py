import json

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
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


class ActivityLogGenerator:
    """Generate e-commerce activity logs with activity types and status"""
    
    def __init__(self, batch_size=10000):
        self.batch_size = batch_size
        self.activity_types = {
            'login': 0.10,           # 10% - login entries
            'logout': 0.05,          # 5% - logout entries
            'cart-add': 0.45,        # 45% - cart additions (majority)
            'cart-update': 0.25,     # 25% - cart updates (majority)
            'cart-remove': 0.05,     # 5% - cart removals
            'checkout': 0.1        # 10% - checkout attempts
        }
        self.categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Beauty', 'Sports']
        self.devices = ['Desktop', 'Mobile', 'Tablet']
        self.regions = ['US', 'EU', 'APAC', 'LATAM']
    
    def get_status_for_activity(self, activity_type):
        """Determine status based on activity type"""
        if activity_type == 'login':
            # Login: 98% success, 2% failed
            return random.choice(['success'] * 98 + ['failed'] * 2)
        
        elif activity_type == 'logout':
            # Logout: almost always success
            return 'success'
        
        elif activity_type in ['cart-add', 'cart-update', 'cart-remove']:
            # Cart operations: 99% success, 1% failed
            return random.choice(['success'] * 99 + ['failed'] * 1)
        
        elif activity_type == 'checkout':
            # Checkout: 30% success, 15% failed, 55% abandoned
            return random.choice(['success'] * 30 + ['failed'] * 15 + ['abandoned'] * 55)

        else:
            return 'success'
    
    def generate_activity_record(self):
        """Generate a single activity log record"""
        
        # Choose activity type based on distribution
        activity_type = random.choices(
            list(self.activity_types.keys()),
            weights=list(self.activity_types.values())
        )[0]
        
        status = self.get_status_for_activity(activity_type)
        
        # Base record
        record = {
            'user_id': str(uuid.uuid4()),
            'session_id': str(uuid.uuid4()),
            'activity_type': activity_type,
            'status': status,
            'timestamp': (datetime.utcnow() - timedelta(days=random.randint(0, 30))).isoformat(),
            'device_type': random.choice(self.devices),
            'region': random.choice(self.regions),
            'ip_address': fake.ipv4(),
            'user_email':  fake.email()
        }
        
        # Add activity-specific details
        if activity_type == 'login':
            record['login_method'] = random.choice(['email', 'google', 'facebook', 'apple'])
            if status == 'failed':
                record['failure_reason'] = random.choice(['invalid_credentials', 'account_locked', 'mfa_failed'])
        
        elif activity_type == 'logout':
            record['logout_reason'] = random.choice(['user_initiated', 'session_timeout'])
        
        elif activity_type == 'cart-add':
            record['product_id'] = str(uuid.uuid4())
            record['product_name'] = f"{fake.word().title()} {fake.word().title()}"
            record['category'] = random.choice(self.categories)
            record['quantity'] = random.randint(1, 5)
            record['cart_price'] = round(random.uniform(10, 500), 2)
            #record['cart_id'] = str(uuid.uuid4())
            if status == 'failed':
                record['failure_reason'] = random.choice(['product_not_found', 'out_of_stock', 'invalid_quantity'])
        
        elif activity_type == 'cart-update':
            record['product_id'] = str(uuid.uuid4())
            record['product_name'] = f"{fake.word().title()} {fake.word().title()}"
            record['quantity'] = random.randint(1, 5)
            #record['new_quantity'] = random.randint(0, 5)
            #record['cart_id'] = str(uuid.uuid4())
            record['cart_price'] = round(random.uniform(10, 500), 2)
            if status == 'failed':
                record['failure_reason'] = random.choice(['insufficient_stock', 'invalid_quantity', 'cart_expired'])
        
        elif activity_type == 'cart-remove':
            record['product_id'] = str(uuid.uuid4())
            record['product_name'] = f"{fake.word().title()} {fake.word().title()}"
            record['quantity'] = random.randint(0, 3)
            record['cart_price'] = round(random.uniform(10, 500), 2)
            #record['cart_id'] = str(uuid.uuid4())
        
        elif activity_type == 'checkout':
            #record['cart_id'] = str(uuid.uuid4())
            record['order_id'] = str(uuid.uuid4()) if status == 'success' else None
            record['cart_total_amount$'] = round(random.uniform(20, 500), 2)
            record['item_count'] = random.randint(1, 10)
            record['payment_method'] = random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay', 'COD'])
            if status == 'failed':
                record['failure_reason'] = random.choice(['card_declined', 'inventory_issue', 'technical_error'])
            else:
                record['checkout_time_seconds'] = random.randint(60, 600)
        
        return record
    
    def generate_batch(self):
        """Generate a batch of activity records"""
        return [self.generate_activity_record() for _ in range(self.batch_size)]


def lambda_handler(event, context):
    """
    Lambda entry point for activity log batch generation
    """
    
    try:
        # Configuration from environment
        bucket = 'shopin-cart-analysis'
        prefix = 'raw'
        batch_size = 1000
        output_format = 'json'
        
        if not bucket:
            raise ValueError("S3_BUCKET environment variable is required")
        
        logger.info(f"Starting batch generation: size={batch_size}, format={output_format}")
        
        # Generate data
        generator = ActivityLogGenerator(batch_size)
        records = generator.generate_batch()
        
        
        # Prepare S3 key with timestamp and batch ID
        batch_id = str(uuid.uuid4())[:8]
        file_name = 'cart_data'
        filedate = datetime.utcnow().strftime('%Y%m%d')
        file_extension = 'jsonl' if output_format == 'jsonl' else 'json'
        s3_key = f"{prefix}/{filedate}/{file_name}_{batch_id}.{file_extension}"
        
        # Format and upload
        if output_format == 'jsonl':
            body = '\n'.join([json.dumps(record) for record in records])
        else:
            body = json.dumps(records, indent=2)
        
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=body,
            ContentType='application/json',
            Metadata={
                'batch-id': batch_id,
                'record-count': str(len(records)),
                'generated-at': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Successfully uploaded batch to s3://{bucket}/{s3_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Batch generated successfully',
                'batch_id': batch_id,
                'record_count': len(records),
                's3_location': f"s3://{bucket}/{s3_key}",
                'timestamp': datetime.utcnow().isoformat()
            })
        }
    
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }
