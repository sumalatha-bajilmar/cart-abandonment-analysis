"""
DAG Name: ecom_cart_trend_analysis
Description: End-to-end analytics pipeline resolving parameters through AWS SSM.
             Extracts raw customer and cart transactional records, identifies the abandoned carts and stores the final data in S3 for further BI reports.
"""

from datetime import datetime, timedelta
import pendulum
import logging
import io
import pandas as pd
import numpy as np

from airflow.decorators import dag, task
#from airflow.models import Variable
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

from utils.cart_utils_reusable import (read_customer_data, merge_cart_records_for_analysis)

AWS_CONN_ID = "aws_default"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ecom_cart_trend_analysis",
    default_args=default_args,
    description="Process e-commerce cart activity logs and identifies if the cart is abandoned",
    schedule="@daily",
    start_date=datetime(2026, 5, 24),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ssm", "parquet", "analytics", "promotional"],
)
def ecom_cart_trend_analysis_pipeline():

    @task()
    def extract_cart_log(logical_date=None, **context) -> pd.DataFrame:
        """
        Dynamically fetches batch transactional JSON rows for the specific execution partition date and adds abandoned flag to each row.
        """
        return merge_cart_records_for_analysis()


    @task()
    def extract_customer_data() -> pd.DataFrame:
        """
        Fetches the primary dimensional profile file containing master customer records.
        """
        return read_customer_data()
        

    @task()
    def aggregate_data() -> str:
        """
        Calculates window-bounded activity loops, combines cart quantity and price
        """
        df_cart = extract_cart_log ()
        if df_cart.empty:
            logging.warning("Input dataset is incomplete or missing. Halting pipeline execution downstream.")
            return None


        # 1. Enforce sorting constraints for chronological calculations
        df = df_cart.sort_values(by=["user_id", "product_id", "timestamp"]).copy()

        # 2. Flag resets & rank backward from the latest event per product group
        df["is_reset"] = df["activity_type"].isin(["cart-update", "cart-remove"])
        df["reset_rnk"] = (
            df[df["is_reset"]]
            .groupby(["user_id", "product_id"])["timestamp"]
            .rank(method="first", ascending=False)
        )

        # 3. Pull latest baseline reset timestamps and values into an accessible join matrix
        latest_resets = df[df["reset_rnk"] == 1][["user_id", "product_id", "timestamp", "activity_type", "quantity"]].rename(
            columns={"timestamp": "reset_time", "activity_type": "reset_action", "quantity": "reset_qty"}
        )
        df = df.merge(latest_resets, on=["user_id", "product_id"], how="left")

        # 4. Isolate active records inside the valid operational window
        is_latest_reset = df["reset_rnk"] == 1
        is_valid_subsequent_add = (df["timestamp"] > df["reset_time"]) & (df["activity_type"] == "cart-add")
        no_reset_ever_happened = df["reset_time"].isna()

        valid_window_df = df[is_latest_reset | is_valid_subsequent_add | no_reset_ever_happened].copy()


        # 5. Compute true quantities (Treat latest remove/update as reset quantity and price)
        valid_window_df["calculated_qty"] = valid_window_df["quantity"]
        valid_window_df["total_product_price"] = valid_window_df["cart_price"]

        # 6. Aggregate to calculate the final volumes per product, then global user totals
        product_summary = (
            valid_window_df.groupby(["user_id", "product_id"])
            .agg(final_quantity=("calculated_qty", "sum"), product_cart_price=("total_product_price", "sum"))
            .reset_index()
        )
        user_summary = (
            product_summary.groupby("user_id")
            .agg(total_quantity=("final_quantity", "sum"), total_cart_price=("product_cart_price", "sum"))
            .reset_index()
        )

        local_path = f"/tmp/user_summary.csv"
    
        # Save it to disk
        user_summary.to_csv(local_path, index=False)

        return local_path

    @task()
    def merge_and_store(df_customers: pd.DataFrame, cart_file_path: str, logical_date=None, **context):
        """
        Merge the cart data with customer data and store in designated S3 bucket path
        """

        active_date = logical_date if logical_date is not None else datetime.now(pendulum.UTC)
        execution_date_str = active_date.strftime("%Y%m%d")

        # Enrich transactional metrics with core metadata profile records

        if os.path.exists(cart_file_path):
            user_summary = pd.read_csv(cart_file_path)
            print(f"Loaded summary with {len(user_summary)} records.")

        final_report = df_customers.merge(user_summary, on="user_id", how="inner")

        # Load compiled report dataset directly to S3 partitioned analytics storage layer
        
        target_bucket = Variable.get("s3_cart_bucket")
        target_key = f"analytics/{execution_date_str}/enriched_cart_data.parquet"
        
        logging.info(f"Uploading final analytical reports to s3://{target_bucket}/{target_key}")
        parquet_buffer = io.BytesIO()
        final_report.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=target_key,
            bucket_name=target_bucket,
            replace=True
        )

        # Deleting the temp file after processing to free up disk space
        os.remove(cart_file_path)

        return f"s3://{target_bucket}/{target_key}"

    # Declare runtime pipeline tasks
    cart_data = extract_cart_log()
    customer_data = extract_customer_data()
    final_cart_data_path = aggregate_data()
    
    # TaskFlow API to form execution sequence based on dependency variables automatically
    merge_and_store(df_customers=customer_data, cart_file_path: final_cart_data_path)

# Instantiate the final production pipeline
ecom_cart_trend_analysis = ecom_cart_trend_analysis_pipeline()
