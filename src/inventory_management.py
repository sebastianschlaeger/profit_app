import pandas as pd
from src.s3_utils import get_s3_fs
import logging
import streamlit as st

logger = logging.getLogger(__name__)

def load_material_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                df['SKU'] = df['SKU'].astype(str)
                df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                return df
        return pd.DataFrame(columns=['SKU', 'Cost', 'Date'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Materialkostendaten: {str(e)}")
        raise

def save_material_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        df['SKU'] = df['SKU'].astype(str)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Materialkostendaten: {str(e)}")
        raise
