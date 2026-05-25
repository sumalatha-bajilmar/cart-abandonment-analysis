"""
DAG Name: ecom_cart_abandonment_processor
Description: End-to-end analytics pipeline resolving parameters through AWS SSM.
             Extracts raw customer and cart transactional records, applies 
             window-reset aggregation rules, and dumps enriched metrics to S3.
"""

from datetime import datetime, timedelta
import logging
import io
import pandas as pd
import numpy as np

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

from cart_utils import (read_customer_data, identify_abandoned_cart)

AWS_CONN_ID = "aws_default"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-alerts@yourcompany.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ecom_cart_abandonment_processor",
    default_args=default_args,
    description="Process e-commerce cart activity logs and customer segmentation",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 25),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "ssm", "parquet", "analytics", "promotional"],
)
def ecom_cart_abandonment_pipeline():

    @task()
    def extract_abandoned_cart(logical_date=None, **context) -> pd.DataFrame:
        """
        Dynamically fetches batch transactional JSON rows for the specific execution partition date.
        """
        return identify_abandoned_cart()


    @task()
    def extract_customer_data() -> pd.DataFrame:
        """
        Fetches the primary dimensional profile file containing master customer records.
        """
        return read_customer_data()
        

    @task()
    def aggregate_and_segment_data(df_cart: pd.DataFrame, df_customers: pd.DataFrame, logical_date=None, **context):
        """
        Calculates window-bounded activity loops, combines customer attributes, and creates customer segments based on cart values.
        """
        if df_cart.empty or df_customers.empty:
            logging.warning("Input datasets are incomplete or missing. Halting pipeline execution downstream.")
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
        valid_window_df["total_product_price"] = valid_window_df["product_price"]

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

        # 7. Apply vectorized business segmentation logic
        conditions = [
            (user_summary["total_quantity"] > 1) & (user_summary["total_cart_price"] >= 1) & (user_summary["total_cart_price"] <= 100),
            (user_summary["total_quantity"] > 1) & (user_summary["total_cart_price"] > 100)
        ]
        choices = ["BucketA", "BucketB"]

        user_summary["cart_status"] = np.where(user_summary["total_quantity"] > 1, "Abandoned", "Active")
        user_summary["price_bucket"] = np.select(conditions, choices, default="Not_Eligible")

        # 8. Enrich transactional metrics with core metadata profile records
        final_report = df_customers.merge(user_summary, on="user_id", how="inner")

        # 9. Load compiled report dataset directly to S3 partitioned analytics storage layer
        
        target_bucket = Variable.get("s3_cart_bucket")
        execution_date_str = logical_date.strftime("%Y%m%d")
        target_key = f"promotional/{execution_date_str}/aggregated_customer_segments.parquet"
        
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
        return f"s3://{target_bucket}/{target_key}"

    # Declare runtime pipeline tasks
    cart_data = extract_abandoned_cart()
    customer_data = extract_customer_data()
    
    # TaskFlow API to form execution sequence based on dependency variables automatically
    aggregate_and_segment_data(df_cart=cart_data, df_customers=customer_data)

# Instantiate the final production pipeline
ecom_cart_abandonment_processor_dag = ecom_cart_abandonment_pipeline()
