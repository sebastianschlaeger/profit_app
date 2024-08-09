import requests
from datetime import datetime, timedelta
import streamlit as st
import logging
import pandas as pd
import json
import csv

logger = logging.getLogger(__name__)

class BillbeeAPI:
    BASE_URL = "https://api.billbee.io/api/v1"

    def __init__(self):
        self.api_key = st.secrets["billbee"]["API_KEY"]
        self.username = st.secrets["billbee"]["USERNAME"]
        self.password = st.secrets["billbee"]["PASSWORD"]

    def get_orders_for_date(self, date):
        endpoint = f"{self.BASE_URL}/orders"
        headers = {
            "X-Billbee-Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
        params = {
            "minOrderDate": date.isoformat(),
            "maxOrderDate": (date + timedelta(days=1)).isoformat(),
            "pageSize": 250  # Max page size
        }

        all_orders = []
        page = 1

        try:
            while True:
                params['page'] = page
                response = requests.get(endpoint, headers=headers, params=params, auth=(self.username, self.password))
                response.raise_for_status()
                data = response.json()

                all_orders.extend(data['Data'])

                if page >= data['Paging']['TotalPages']:
                    break

                page += 1

            logger.info(f"Successfully retrieved {len(all_orders)} orders for date {date}")
            return all_orders

        except requests.RequestException as e:
            error_msg = f"Fehler bei der Anfrage an Billbee API: {str(e)}"
            logger.error(error_msg)
            st.error(error_msg)
            raise

def process_orders(orders_data):
    processed_orders = []
    for order in orders_data:
        processed_order = {
            "BillbeeID": order["BillBeeOrderId"],
            "OrderItems": [],
            "Platform": order["Seller"]["Platform"],
            "CustomerCountry": order["ShippingAddress"]["CountryISO2"],
            "TotalOrderPrice": 0,
            "TotalOrderWeight": 0,
            "Currency": order["Currency"],
            "CreatedAt": order["CreatedAt"].split("T")[0],
            "TaxAmount": sum(item["TaxAmount"] for item in order["OrderItems"]),
            "TotalCost": order["TotalCost"]
        }

        for item in order["OrderItems"]:
            order_item = {
                "SKU": item["Product"]["SKU"],
                "Quantity": item["Quantity"],
                "TotalPrice": item["TotalPrice"],
                "Weight": item["Product"]["Weight"]
            }
            processed_order["OrderItems"].append(order_item)
            processed_order["TotalOrderPrice"] += item["TotalPrice"]
            processed_order["TotalOrderWeight"] += item["Product"]["Weight"] * item["Quantity"]

        processed_orders.append(processed_order)

    return processed_orders

def save_to_csv(processed_orders, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ["BillbeeID", "Platform", "CustomerCountry", "TotalOrderPrice", "TotalOrderWeight", "Currency", "CreatedAt", "TaxAmount", "TotalCost", "OrderItems"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for order in processed_orders:
            order_copy = order.copy()
            order_copy["OrderItems"] = json.dumps(order_copy["OrderItems"])
            writer.writerow(order_copy)

def main():
    billbee_api = BillbeeAPI()
    
    # Beispiel: Abrufen der Daten für gestern
    yesterday = datetime.now().date() - timedelta(days=1)
    orders_data = billbee_api.get_orders_for_date(yesterday)
    
    processed_orders = process_orders(orders_data)
    
    # Speichern der verarbeiteten Daten in einer CSV-Datei
    filename = f"billbee_orders_{yesterday.strftime('%Y-%m-%d')}.csv"
    save_to_csv(processed_orders, filename)
    
    logger.info(f"Processed orders saved to {filename}")

if __name__ == "__main__":
    main()
