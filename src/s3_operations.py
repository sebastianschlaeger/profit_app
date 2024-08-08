import pandas as pd
from src.s3_utils import get_s3_fs
import logging

logger = logging.getLogger(__name__)

SALES_FILE = "all_sales_data_profit_app.csv"

def save_to_s3(new_data, date):
    """Speichert neue Verkaufsdaten in S3."""
    try:
        s3 = get_s3_fs()
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        full_path = f"{bucket_name}/{SALES_FILE}"
        
        if s3.exists(full_path):
            existing_data = load_existing_data(s3, full_path)
            combined_data = combine_data(existing_data, new_data, date)
        else:
            combined_data = prepare_new_data(new_data, date)
        
        save_combined_data(s3, full_path, combined_data)
        return SALES_FILE
    except Exception as e:
        logger.error(f"Fehler beim Speichern in S3: {str(e)}")
        raise


def get_saved_dates(days=30):
    """Holt gespeicherte Daten aus S3."""
    try:
        s3 = get_s3_fs()
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        full_path = f"{bucket_name}/{SALES_FILE}"
        
        if s3.exists(full_path):
            all_data = load_existing_data(s3, full_path)
            return set(pd.to_datetime(all_data['Date']).dt.date)
        return set()
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der gespeicherten Daten: {str(e)}")
        return set()

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


def get_all_data_since_date(start_date):
    """Holt alle Daten seit einem bestimmten Datum."""
    try:
        s3 = get_s3_fs()
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        full_path = f"{bucket_name}/{SALES_FILE}"
        
        if s3.exists(full_path):
            all_data = load_existing_data(s3, full_path)
            all_data['Date'] = pd.to_datetime(all_data['Date'])
            return all_data[all_data['Date'] >= pd.to_datetime(start_date)]
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Daten: {str(e)}")
        return pd.DataFrame()
