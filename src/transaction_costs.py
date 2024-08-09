import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def load_transaction_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/transaction_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                return df
        return pd.DataFrame(columns=['Platform', 'TransactionCostPercent'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Transaktionskostendaten: {str(e)}")
        raise

def save_transaction_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/transaction_costs.csv"
    try:
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Transaktionskostendaten: {str(e)}")
        raise
