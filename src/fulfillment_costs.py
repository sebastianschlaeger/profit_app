import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def load_fulfillment_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/fulfillment_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                return df
        return pd.DataFrame(columns=['Auftragspauschale', 'SKU_Pick', 'Kartonage', 'Versandkosten'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Fulfillment-Kostendaten: {str(e)}")
        raise

def save_fulfillment_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/fulfillment_costs.csv"
    try:
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Fulfillment-Kostendaten: {str(e)}")
        raise
