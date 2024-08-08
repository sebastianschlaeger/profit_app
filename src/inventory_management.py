import pandas as pd
from src.s3_utils import get_s3_fs

def load_supplier_deliveries():
    s3 = get_s3_fs()
    if s3.exists('supplier_deliveries.csv'):
        with s3.open('supplier_deliveries.csv', 'rb') as f:
            return pd.read_csv(f)
    return pd.DataFrame(columns=['SKU', 'Cost', 'Date'])

def save_supplier_deliveries(df):
    s3 = get_s3_fs()
    with s3.open('supplier_deliveries.csv', 'w') as f:
        df.to_csv(f, index=False)
