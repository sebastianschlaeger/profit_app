import s3fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def get_s3_fs():
    try:
        return s3fs.S3FileSystem(
            key=st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
            secret=st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
            client_kwargs={
                'region_name': st.secrets["aws"]["AWS_DEFAULT_REGION"]
            }
        )
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der S3-Verbindung: {str(e)}")
        raise
