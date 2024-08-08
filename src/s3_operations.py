import pandas as pd
from src.s3_utils import get_s3_fs

def save_to_s3(df, date):
    s3 = get_s3_fs()
    filename = f"orders_{date.strftime('%Y-%m-%d')}.csv"
    with s3.open(filename, 'w') as f:
        df.to_csv(f, index=False)

def get_saved_dates():
    s3 = get_s3_fs()
    files = s3.ls('/')
    return [pd.to_datetime(f.split('_')[1].split('.')[0]).date() for f in files if f.startswith('orders_')]

def load_from_s3(date):
    s3 = get_s3_fs()
    filename = f"orders_{date.strftime('%Y-%m-%d')}.csv"
    if s3.exists(filename):
        with s3.open(filename, 'rb') as f:
            return pd.read_csv(f)
    return None
