import requests
from datetime import datetime, timedelta
import streamlit as st
import logging

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
                logger.info(f"Requesting page {page} for date {date}")
                response = requests.get(endpoint, headers=headers, params=params, auth=(self.username, self.password))
                response.raise_for_status()
                data = response.json()

                logger.info(f"Received {len(data['Data'])} orders on page {page}")
                all_orders.extend(data['Data'])

                logger.info(f"Current page: {data['Paging']['Page']}, Total pages: {data['Paging']['TotalPages']}")
                if page >= data['Paging']['TotalPages']:
                    break

                page += 1

            logger.info(f"Successfully retrieved {len(all_orders)} orders for date {date}")
            logger.debug(f"Order IDs retrieved: {[order['Id'] for order in all_orders]}")
            return all_orders

        except requests.RequestException as e:
            error_msg = f"Error in request to Billbee API: {str(e)}"
            logger.error(error_msg, exc_info=True)
            st.error(error_msg)
            raise

billbee_api = BillbeeAPI()
