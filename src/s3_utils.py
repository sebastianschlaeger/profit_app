import s3fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

logger.info("s3_utils.py wird geladen")

def get_s3_fs():
    logger.info("Starte get_s3_fs Funktion")
    try:
        logger.info("Versuche, S3 FileSystem Objekt zu erstellen")
        
        # Loggen der S3-Konfiguration (Vorsicht mit sensiblen Daten!)
        logger.info(f"AWS Region: {st.secrets['aws']['AWS_DEFAULT_REGION']}")
        logger.info(f"S3 Bucket Name: {st.secrets['aws']['S3_BUCKET_NAME']}")
        
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
            secret=st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
            client_kwargs={
                'region_name': st.secrets["aws"]["AWS_DEFAULT_REGION"]
            }
        )
        
        logger.info("S3 FileSystem Objekt erfolgreich erstellt")
        return s3
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der S3-Verbindung: {str(e)}", exc_info=True)
        raise
