import pandas as pd
from src.s3_utils import get_s3_fs
import logging
import streamlit as st

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
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    filename = f"daily_orders_{date.strftime('%Y-%m-%d')}.csv"
    full_path = f"{bucket_name}/{filename}"
    
    logger.info(f"Versuche, Datei zu laden: {full_path}")
    
    try:
        if s3.exists(full_path):
            logger.info(f"Datei gefunden: {full_path}")
            with s3.open(full_path, 'rb') as f:
                df = pd.read_csv(f)
            logger.info(f"Datei erfolgreich geladen. Anzahl der Zeilen: {len(df)}")
            return df
        else:
            logger.warning(f"Datei nicht gefunden: {full_path}")
            return None
    except Exception as e:
        logger.error(f"Fehler beim Laden der Datei {full_path}: {str(e)}")
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

def save_daily_order_data(df, date):
    """Speichert tägliche Bestelldaten in S3."""
    try:
        s3 = get_s3_fs()
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        file_name = f"daily_orders_{date.strftime('%Y-%m-%d')}.csv"
        full_path = f"{bucket_name}/{file_name}"
        
        with s3.open(full_path, 'w') as f:
            df.to_csv(f, index=False)
        
        logger.info(f"Tägliche Bestelldaten für {date} erfolgreich in S3 gespeichert.")
    except Exception as e:
        logger.error(f"Fehler beim Speichern der täglichen Bestelldaten in S3: {str(e)}")
        raise

def load_existing_data(s3, file_path):
    """Lädt existierende Daten aus S3."""
    with s3.open(file_path, 'rb') as f:
        return pd.read_csv(f)

def prepare_new_data(new_data, date):
    """Bereitet neue Daten für das Speichern vor."""
    new_data['Date'] = date
    return new_data

def combine_data(existing_data, new_data, date):
    """Kombiniert existierende und neue Daten."""
    new_data['Date'] = date
    return pd.concat([existing_data, new_data], ignore_index=True)

def save_combined_data(s3, file_path, data):
    """Speichert kombinierte Daten in S3."""
    with s3.open(file_path, 'w') as f:
        data.to_csv(f, index=False)
