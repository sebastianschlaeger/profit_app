import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging
import io
import sys

# Konfigurieren Sie das Logging direkt in dieser Datei
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])

logger = logging.getLogger(__name__)

logger.info("inventory_management.py wird geladen")

def load_material_costs():
    logger.info("Starte load_material_costs Funktion")
    try:
        logger.info("Versuche, get_s3_fs aufzurufen")
        s3 = get_s3_fs()
        logger.info("S3 FileSystem Objekt erfolgreich erstellt")
    except Exception as e:
        logger.error(f"Fehler beim Erstellen des S3 FileSystem Objekts: {str(e)}", exc_info=True)
        return pd.DataFrame(columns=['SKU', 'Cost'])

    try:
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        logger.info(f"S3 Bucket Name: {bucket_name}")
    except Exception as e:
        logger.error(f"Fehler beim Zugriff auf S3 Bucket Name: {str(e)}", exc_info=True)
        return pd.DataFrame(columns=['SKU', 'Cost'])

    file_path = f"{bucket_name}/material_costs.csv"
    logger.info(f"Vollständiger S3-Pfad: {file_path}")
    
    try:
        logger.info(f"Versuche, Inhalt des Buckets zu listen: {bucket_name}")
        files_in_bucket = s3.ls(bucket_name)
        logger.info(f"Dateien im Bucket: {files_in_bucket}")
        
        logger.info(f"Überprüfe Existenz der Datei: {file_path}")
        if s3.exists(file_path):
            logger.info(f"Datei gefunden: {file_path}")
            with s3.open(file_path, 'rb') as f:
                content = f.read().decode('utf-8')
                logger.info(f"Dateiinhalt (erste 100 Zeichen): {content[:100]}")
                df = pd.read_csv(io.StringIO(content))
            
            logger.info(f"DataFrame erstellt. Spalten: {df.columns}, Zeilen: {len(df)}")
            df['SKU'] = df['SKU'].astype(str)
            return df
        else:
            logger.warning(f"Die Datei {file_path} wurde nicht gefunden.")
            return pd.DataFrame(columns=['SKU', 'Cost'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Materialkostendaten: {str(e)}", exc_info=True)
        return pd.DataFrame(columns=['SKU', 'Cost'])


def save_material_costs(df):
    logger.info("Starte save_material_costs Funktion")
    s3 = get_s3_fs()
    logger.debug(f"S3 FileSystem Objekt erstellt: {s3}")
    
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    logger.info(f"Versuche, Datei zu speichern: {file_path}")
    
    try:
        logger.debug("Konvertiere SKU zu String")
        df['SKU'] = df['SKU'].astype(str)
        
        logger.debug("Erstelle CSV-String")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        logger.debug(f"CSV-Inhalt (erste 100 Zeichen): {csv_content[:100]}")
        
        logger.debug(f"Öffne Datei zum Schreiben: {file_path}")
        with s3.open(file_path, 'w') as f:
            f.write(csv_content)
        
        logger.info(f"Materialkostendaten erfolgreich in {file_path} gespeichert.")
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Materialkostendaten: {str(e)}")
        logger.error(f"Stacktrace: {traceback.format_exc()}")
        raise
