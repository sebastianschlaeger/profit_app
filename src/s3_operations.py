import pandas as pd
from src.s3_utils import get_s3_fs
import logging

logger = logging.getLogger(__name__)

def save_to_s3(df, date):
    s3 = get_s3_fs()
    filename = f"orders_{date.strftime('%Y-%m-%d')}.csv"
    try:
        with s3.open(filename, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Daten in S3: {str(e)}")
        raise

def get_saved_dates():
    s3 = get_s3_fs()
    try:
        files = s3.ls('/')
        return [pd.to_datetime(f.split('_')[1].split('.')[0]).date() for f in files if f.startswith('orders_')]
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der gespeicherten Daten: {str(e)}")
        raise

def load_from_s3(date):
    s3 = get_s3_fs()
    filename = f"orders_{date.strftime('%Y-%m-%d')}.csv"
    try:
        if s3.exists(filename):
            with s3.open(filename, 'rb') as f:
                return pd.read_csv(f)
        return None
    except Exception as e:
        logger.error(f"Fehler beim Laden der Daten aus S3: {str(e)}")
        raise
