import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract_billbee_data(order):
    return {
        "OrderId": order["Id"],
        "OrderNumber": order["OrderNumber"],
        "CreatedAt": order["CreatedAt"],
        "TotalCost": order["TotalCost"],
        "ShippingCost": order["ShippingCost"],
        "Currency": order["Currency"],
        "PaymentMethod": order["PaymentMethod"],
        "OrderItems": [{
            "ProductId": item["Product"]["Id"],
            "SKU": item["Product"]["SKU"],
            "Quantity": item["Quantity"],
            "TotalPrice": item["TotalPrice"],
            "TaxAmount": item["TaxAmount"],
        } for item in order["OrderItems"]],
        "ShippingAddress": {
            "Country": order["ShippingAddress"]["Country"],
            "CountryISO2": order["ShippingAddress"]["CountryISO2"],
        },
        "ShippingProviderId": order["ShippingProviderId"],
        "ShippingProviderProductId": order["ShippingProviderProductId"],
    }

def process_orders(orders_data):
    try:
        processed_data = []
        for order in orders_data:
            extracted_order = extract_billbee_data(order)
            for item in extracted_order['OrderItems']:
                processed_data.append({
                    'OrderId': extracted_order['OrderId'],
                    'OrderNumber': extracted_order['OrderNumber'],
                    'CreatedAt': extracted_order['CreatedAt'],
                    'TotalCost': extracted_order['TotalCost'],
                    'ShippingCost': extracted_order['ShippingCost'],
                    'Currency': extracted_order['Currency'],
                    'PaymentMethod': extracted_order['PaymentMethod'],
                    'ProductId': item['ProductId'],
                    'SKU': item['SKU'],
                    'Quantity': item['Quantity'],
                    'TotalPrice': item['TotalPrice'],
                    'TaxAmount': item['TaxAmount'],
                    'ShippingCountry': extracted_order['ShippingAddress']['Country'],
                    'ShippingCountryISO2': extracted_order['ShippingAddress']['CountryISO2'],
                    'ShippingProviderId': extracted_order['ShippingProviderId'],
                    'ShippingProviderProductId': extracted_order['ShippingProviderProductId'],
                })
        return pd.DataFrame(processed_data)
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Bestelldaten: {str(e)}")
        raise
