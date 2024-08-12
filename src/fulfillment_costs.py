import pandas as pd
from src.s3_utils import get_s3_fs
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def load_fulfillment_costs():
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/fulfillment_costs.csv"
    try:
        if s3.exists(file_path):
            with s3.open(file_path, 'rb') as f:
                df = pd.read_csv(f)
                return df
        return pd.DataFrame(columns=['Auftragspauschale', 'SKU_Pick', 'Kartonage', 'Versandkosten'])
    except Exception as e:
        logger.error(f"Fehler beim Laden der Fulfillment-Kostendaten: {str(e)}")
        raise

def save_fulfillment_costs(df):
    s3 = get_s3_fs()
    bucket_name = st.secrets['aws']['S3_BUCKET_NAME']
    file_path = f"{bucket_name}/fulfillment_costs.csv"
    try:
        with s3.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Fulfillment-Kostendaten: {str(e)}")
        raise

def calculate_shipping_costs(weight_grams, country):
    weight_kg = weight_grams / 1000

    def calculate_germany_shipping(weight):
        base_price = 0
        if weight <= 2:
            base_price = 3.55
        elif weight <= 3:
            base_price = 3.65
        elif weight <= 5:
            base_price = 3.90
        elif weight <= 20:
            base_price = 6.90
        else:
            base_price = 9.90

        maut_co2_zuschlag = 0.18
        energy_zuschlag = base_price * 0.0125
        return base_price + maut_co2_zuschlag + energy_zuschlag

    def calculate_austria_shipping(weight):
        return 6.68 + 0.40 * weight

    def calculate_other_shipping(weight):
        return 11.6 + 0.70 * weight

    if country == 'DE':
        return calculate_germany_shipping(weight_kg)
    elif country == 'AT':
        return calculate_austria_shipping(weight_kg)
    else:
        return calculate_other_shipping(weight_kg)
