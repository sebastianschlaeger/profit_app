import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging
from src.error_handler import handle_error, DataFetchError

logger = logging.getLogger(__name__)

def load_marketing_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/marketing_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                return df
        return pd.DataFrame(columns=['Date', 'Google Ads', 'Amazon Ads', 'Ebay Ads', 'Kaufland Ads'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Marketingkostendaten: {str(e)}")
        raise

def save_marketing_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/marketing_costs.csv"
    try:
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Marketingkostendaten: {str(e)}")
        raise
