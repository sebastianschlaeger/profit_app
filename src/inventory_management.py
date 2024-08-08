import pandas as pd
from src.s3_utils import get_s3_fs
import logging

logger = logging.getLogger(__name__)

def load_supplier_deliveries():
    s3 = get_s3_fs()
    try:
        if s3.exists('supplier_deliveries.csv'):
            with s3.open('supplier_deliveries.csv', 'rb') as f:
                df = pd.read_csv(f)
                # Ensure SKU is treated as string
                df['SKU'] = df['SKU'].astype(str)
                return df
        return pd.DataFrame(columns=['SKU', 'Cost', 'Date'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Lieferantendaten: {str(e)}")
        raise

def save_supplier_deliveries(df):
    s3 = get_s3_fs()
    try:
        # Ensure SKU is treated as string before saving
        df['SKU'] = df['SKU'].astype(str)
        with s3.open('supplier_deliveries.csv', 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Lieferantendaten: {str(e)}")
        raise
