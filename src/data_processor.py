import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

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
                "Weight": item["Product"]["Weight"] or 0  # Use 0 if Weight is None
            }
            processed_order["OrderItems"].append(order_item)
            processed_order["TotalOrderPrice"] += item["TotalPrice"]
            processed_order["TotalOrderWeight"] += (item["Product"]["Weight"] or 0) * item["Quantity"]

        processed_orders.append(processed_order)

    return processed_orders

def prepare_data_for_csv(processed_orders):
    csv_data = []
    for order in processed_orders:
        order_copy = order.copy()
        order_copy["OrderItems"] = json.dumps(order_copy["OrderItems"])
        csv_data.append(order_copy)
    return csv_data

def create_dataframe(processed_orders):
    df = pd.DataFrame(prepare_data_for_csv(processed_orders))
    return df

def save_to_csv(df, filename):
    df.to_csv(filename, index=False)
    logger.info(f"Data saved to {filename}")
