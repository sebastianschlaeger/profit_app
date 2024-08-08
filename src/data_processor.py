import pandas as pd
import logging

logger = logging.getLogger(__name__)

def process_orders(orders_data):
    try:
        processed_data = []
        for order in orders_data['Data']:
            for item in order['OrderItems']:
                processed_data.append({
                    'OrderId': order['Id'],
                    'OrderDate': order['CreatedAt'],
                    'SKU': item['SKU'],
                    'Quantity': item['Quantity'],
                    'GrossAmount': item['TotalPrice'],
                })
        return pd.DataFrame(processed_data)
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Bestelldaten: {str(e)}")
        raise
