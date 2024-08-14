import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging
import io

logger = logging.getLogger(__name__)

def load_material_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        logger.info(f"Versuche, Datei zu laden: {file_path}")
        if s3.exists(file_path):
            logger.info(f"Datei gefunden: {file_path}")
            with s3.open(file_path, 'rb') as f:
                content = f.read().decode('utf-8')
                logger.info(f"Dateiinhalt: {content[:100]}...")  # Log the first 100 characters
                df = pd.read_csv(io.StringIO(content))
            logger.info(f"Dataframe erstellt. Spalten: {df.columns}, Zeilen: {len(df)}")
            df['SKU'] = df['SKU'].astype(str)
            return df
        else:
            logger.warning(f"Die Datei {file_path} wurde nicht gefunden.")
            return pd.DataFrame(columns=['SKU', 'Cost'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Materialkostendaten: {str(e)}", exc_info=True)
        return pd.DataFrame(columns=['SKU', 'Cost'])

def save_material_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        df['SKU'] = df['SKU'].astype(str)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        with s3.open(file_path, 'w') as f:
            f.write(csv_content)
        logger.info(f"Materialkostendaten erfolgreich in {file_path} gespeichert. Inhalt: {csv_content[:100]}...")
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Materialkostendaten: {str(e)}", exc_info=True)
        raise
