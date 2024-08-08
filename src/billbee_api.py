import requests
from datetime import datetime

class BillbeeAPI:
    def __init__(self, api_key, username, password):
        self.api_key = api_key
        self.username = username
        self.password = password
        self.base_url = "https://app.billbee.io/api/v1"

    def get_orders_for_date(self, date):
        endpoint = f"{self.base_url}/orders"
        params = {
            "minOrderDate": date.strftime("%Y-%m-%d"),
            "maxOrderDate": date.strftime("%Y-%m-%d"),
        }
        headers = {
            "X-Billbee-Api-Key": self.api_key,
        }
        auth = (self.username, self.password)

        response = requests.get(endpoint, params=params, headers=headers, auth=auth)
        response.raise_for_status()
        return response.json()
