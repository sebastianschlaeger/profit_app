import requests
from datetime import datetime, timedelta
import streamlit as st
import logging
from src.error_handler import handle_error, DataFetchError

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

billbee_api = BillbeeAPI()
