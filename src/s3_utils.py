import s3fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def get_s3_fs():
    try:
        logger.info("Versuche, S3 FileSystem Objekt zu erstellen")
        
        # Loggen der S3-Konfiguration (Vorsicht mit sensiblen Daten!)
        logger.debug(f"AWS Region: {st.secrets['aws']['AWS_DEFAULT_REGION']}")
        logger.debug(f"S3 Bucket Name: {st.secrets['aws']['S3_BUCKET_NAME']}")
        
        s3 = s3fs.S3FileSystem(
            key=st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
            secret=st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
            client_kwargs={
                'region_name': st.secrets["aws"]["AWS_DEFAULT_REGION"]
            }
        )
        
        # Überprüfen Sie, ob die Verbindung funktioniert
        bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
        logger.info(f"Versuche, Inhalt des Buckets zu listen: {bucket_name}")
        bucket_contents = s3.ls(bucket_name)
        logger.info(f"Bucket Inhalt: {bucket_contents}")
        
        logger.info("S3 FileSystem Objekt erfolgreich erstellt und getestet")
        return s3
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der S3-Verbindung: {str(e)}", exc_info=True)
        raise
