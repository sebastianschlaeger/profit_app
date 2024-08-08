import streamlit as st
from datetime import datetime, timedelta
import logging
from src.billbee_api import BillbeeAPI
from src.data_processor import process_orders
from src.inventory_management import load_material_costs, save_material_costs
from src.s3_operations import save_to_s3, get_saved_dates, load_from_s3
from src.s3_utils import get_s3_fs

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="E-Commerce Profitabilitäts-App", layout="wide")

# Initialize BillbeeAPI
billbee_api = BillbeeAPI(
    st.secrets["billbee"]["API_KEY"],
    st.secrets["billbee"]["USERNAME"],
    st.secrets["billbee"]["PASSWORD"]
)

def fetch_yesterday_data():
    yesterday = datetime.now().date() - timedelta(days=1)
    if yesterday not in get_saved_dates():
        try:
            orders_data = billbee_api.get_orders_for_date(yesterday)
            df = process_orders(orders_data)
            save_to_s3(df, yesterday)
            st.success(f"Daten für {yesterday} erfolgreich abgerufen und gespeichert.")
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Daten für {yesterday}: {str(e)}")
            st.error(f"Fehler beim Abrufen der Daten für {yesterday}. Bitte überprüfen Sie die Logs für weitere Details.")
    else:
        logger.warning(f"Daten für {yesterday} wurden bereits importiert.")
        st.info(f"Daten für {yesterday} wurden bereits importiert.")

def manage_material_costs():
    st.subheader("Materialkosten verwalten")
    
    try:
        costs = load_material_costs()
        
        # Convert 'Date' to string for display
        costs['Date'] = costs['Date'].astype(str)
        
        edited_df = st.data_editor(
            costs,
            column_config={
                "SKU": st.column_config.TextColumn("SKU"),
                "Cost": st.column_config.NumberColumn("Materialkosten", min_value=0, step=0.01),
                "Date": st.column_config.DateColumn("Gültig ab Datum"),
            },
            num_rows="dynamic"
        )
        
        if st.button("Änderungen speichern"):
            # Convert 'Date' back to datetime before saving
            edited_df['Date'] = pd.to_datetime(edited_df['Date']).dt.date
            save_material_costs(edited_df)
            st.success("Änderungen wurden gespeichert.")
    except Exception as e:
        logger.error(f"Fehler beim Verwalten der Materialkosten: {str(e)}")
        st.error("Fehler beim Verwalten der Materialkosten. Bitte überprüfen Sie die Logs für weitere Details.")

def get_material_cost_for_date(sku, date):
    try:
        deliveries = load_supplier_deliveries()
        
        # Convert the date column to datetime
        deliveries['Date'] = pd.to_datetime(deliveries['Date'])
        
        # Filter deliveries by date
        relevant_deliveries = deliveries[deliveries['Date'] <= date]
        
        if relevant_deliveries.empty:
            logger.warning(f"Keine Materialkosten gefunden für Datum {date}")
            return None
        
        # Sort deliveries by date in descending order
        relevant_deliveries = relevant_deliveries.sort_values('Date', ascending=False)
        
        # Try to find an exact match first
        exact_match = relevant_deliveries[relevant_deliveries['SKU'] == sku]
        if not exact_match.empty:
            return exact_match.iloc[0]['Cost']
        
        # If no exact match, try partial match
        for _, row in relevant_deliveries.iterrows():
            if sku.startswith(row['SKU']):
                logger.info(f"Partielle Übereinstimmung gefunden: SKU {sku} entspricht Material-SKU {row['SKU']}")
                return row['Cost']
        
        logger.warning(f"Keine Materialkosten gefunden für SKU {sku} am {date}")
        return None
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Materialkosten für SKU {sku} am {date}: {str(e)}")
        return None

def display_overview_table():
    st.subheader("Übersichtstabelle (Letzten 30 Tage)")
    
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=29)
    
    data = {}
    for single_date in (start_date + timedelta(n) for n in range(30)):
        try:
            df = load_from_s3(single_date)
            if df is not None:
                gross_revenue = df['GrossAmount'].sum()
                net_revenue = gross_revenue / 1.19  # Assuming 19% VAT
                material_costs = df.apply(lambda row: (get_material_cost_for_date(row['SKU'], single_date) or 0) * row['Quantity'], axis=1).sum()
                
                data[single_date] = {
                    'Umsatz Brutto': gross_revenue,
                    'Umsatz Netto': net_revenue,
                    'Materialkosten €': material_costs,
                    'Materialkosten %': (material_costs / net_revenue) * 100 if net_revenue > 0 else 0,
                    'Deckungsbeitrag 1': net_revenue - material_costs
                }
        except Exception as e:
            logger.error(f"Fehler beim Verarbeiten der Daten für {single_date}: {str(e)}")
    
    if data:
        df_overview = pd.DataFrame(data).T
        st.dataframe(df_overview)
    else:
        logger.warning("Keine Daten für den ausgewählten Zeitraum verfügbar.")
        st.warning("Keine Daten für den ausgewählten Zeitraum verfügbar.")

def main():
    st.title("E-Commerce Profitabilitäts-App")
    
    if st.button("Daten von gestern abrufen"):
        fetch_yesterday_data()
    
    display_overview_table()
    manage_material_costs()

if __name__ == "__main__":
    main()
