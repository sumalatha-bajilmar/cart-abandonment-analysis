from datetime import datetime, timedelta
import logging
import io
import pandas as pd
import numpy as np


AWS_CONN_ID = "aws_default"



def extract_cart_activity(logical_date=None, **context) -> pd.DataFrame:
    """
    Dynamically fetches batch transactional JSON rows for the specific execution partition date.
    """
        
    source_bucket = Variable.get("s3_cart_bucket")
    execution_date_str = logical_date.strftime("%Y%m%d")
    prefix = f"raw/{execution_date_str}/"
        
    logging.info(f"Connecting via {AWS_CONN_ID} to download cart files from s3://{source_bucket}/{prefix}")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = s3_hook.list_keys(bucket_name=source_bucket, prefix=prefix)

    if not keys:
        logging.warning(f"No cart logs discovered for path metadata partition: {execution_date_str}")
        return pd.DataFrame()

    valid_keys = [k for k in keys if not k.endswith("/")]
    df_list = []
    for key in valid_keys:
        file_obj = s3_hook.get_key(key=key, bucket_name=source_bucket)
        content = file_obj.get()["Body"].read().decode("utf-8")
        df_list.append(pd.read_json(io.StringIO(content), lines=True))

        return pd.concat(df_list, ignore_index=True)

    
def extract_customer_data() -> pd.DataFrame:
    """
    Fetches the primary dimensional profile file containing master customer records.
    """

    customer_bucket = Variable.get("s3_customer_bucket")
    customer_key = "prod/customer_master.json"
        
    logging.info(f"Fetching customer records from s3://{customer_bucket}/{customer_key}")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
    if not s3_hook.check_for_key(key=customer_key, bucket_name=customer_bucket):
        raise AirflowException(f"Missing master dimension table metadata constraint: {customer_key}")
            
    file_obj = s3_hook.get_key(key=customer_key, bucket_name=customer_bucket)
    content = file_obj.get()["Body"].read().decode("utf-8")
        
    return pd.read_json(io.StringIO(content), lines=True)


def identify_abandoned_cart() -> pd.DataFrame:
    """
    Identifies customers who have active cart modifications (add, update, remove)
    but have no subsequent checkout transaction with a 'success' status.
    
    Returns the exact audit trail of those abandoned cart actions.
    """
    cart_activity = extract_cart_activity()

    if cart_activity.empty:
        return pd.DataFrame()
        
    #  Enforce rigorous chronological sorting per customer
    df = cart_activity.sort_values(by=["user_id", "timestamp"]).copy()
    
    #  Identify 'success' checkouts
    df["is_success_checkout"] = (df["activity_type"] == "checkout") & (df["status"] == "success")
    
    #  Find the timestamp of the LAST successful checkout for each user
    # If a user has never successfully checked out, this will be NaT (Not a Time)
    last_checkout_time = (
        df[df["is_success_checkout"]]
        .groupby("user_id")["timestamp"]
        .max()
        .reset_index(name="last_successful_checkout_timestamp")
    )
    
    #  Map the checkpoint matrix back to the master streaming logs
    df = df.merge(last_checkout_time, on="user_id", how="left")
    
    #  Isolate operational records matching your abandonment conditions:
    # Condition A: The action is part of core tracking modifications (add, update, remove)
    # Condition B: The action happened AFTER their last successful checkout 
    #              (OR they have never checked out at all, meaning last_checkout is Null)
    is_cart_modification = df["activity_type"].isin(["cart-add", "cart-update", "cart-remove"])
    occurred_after_last_checkout = (
        df["last_successful_checkout_timestamp"].isna() | 
        (df["timestamp"] > df["last_successful_checkout_timestamp"])
    )
    
    abandoned_actions_df = df[is_cart_modification & occurred_after_last_checkout].copy()
    
    # Clean up internal tracking metadata headers before returning
    drop_cols = ["is_success_checkout", "last_successful_checkout_timestamp"]
    abandoned_cart_df = abandoned_actions_df.drop(columns=drop_cols, errors="ignore")
    
    return abandoned_cart_df
