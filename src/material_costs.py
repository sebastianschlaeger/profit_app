import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def load_material_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                df['SKU'] = df['SKU'].astype(str)
                return df
        return pd.DataFrame(columns=['SKU', 'Cost'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Materialkostendaten: {str(e)}")
        raise

def save_material_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/material_costs.csv"
    try:
        df['SKU'] = df['SKU'].astype(str)
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Materialkostendaten: {str(e)}")
        raise

def manage_material_costs():
    st.subheader("Materialkosten verwalten")
    
    costs = load_material_costs()
    
    edited_df = st.data_editor(
        costs,
        column_config={
            "SKU": st.column_config.TextColumn("SKU"),
            "Cost": st.column_config.NumberColumn("Materialkosten", min_value=0, step=0.01),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_material_costs(edited_df)
        st.success("Änderungen wurden erfolgreich gespeichert.")
