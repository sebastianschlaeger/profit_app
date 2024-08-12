import logging
import streamlit as st

logger = logging.getLogger(__name__)

class AppError(Exception):
    """Base class for application-specific errors."""
    pass

class DataFetchError(AppError):
    """Raised when there's an error fetching data."""
    pass

class DataProcessingError(AppError):
    """Raised when there's an error processing data."""
    pass

class StorageError(AppError):
    """Raised when there's an error with data storage."""
    pass

def handle_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DataFetchError as e:
            logger.error(f"Fehler beim Abrufen der Daten: {str(e)}")
            st.error(f"Fehler beim Abrufen der Daten: {str(e)}")
        except DataProcessingError as e:
            logger.error(f"Fehler bei der Datenverarbeitung: {str(e)}")
            st.error(f"Fehler bei der Datenverarbeitung: {str(e)}")
        except StorageError as e:
            logger.error(f"Fehler beim Speichern der Daten: {str(e)}")
            st.error(f"Fehler beim Speichern der Daten: {str(e)}")
        except Exception as e:
            logger.error(f"Ein unerwarteter Fehler ist aufgetreten: {str(e)}")
            st.error("Ein unerwarteter Fehler ist aufgetreten. Bitte versuchen Sie es später erneut.")
    return wrapper
