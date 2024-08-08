import pandas as pd

def process_orders(orders_data):
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
