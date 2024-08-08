import streamlit as st
from datetime import datetime, timedelta
from src.billbee_api import BillbeeAPI
from src.data_processor import process_orders
from src.inventory_management import load_supplier_deliveries, save_supplier_deliveries
from src.s3_operations import save_to_s3, get_saved_dates, load_from_s3
from src.s3_utils import get_s3_fs

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
        orders_data = billbee_api.get_orders_for_date(yesterday)
        df = process_orders(orders_data)
        save_to_s3(df, yesterday)
        st.success(f"Daten für {yesterday} erfolgreich abgerufen und gespeichert.")
    else:
        st.info(f"Daten für {yesterday} wurden bereits importiert.")

def manage_material_costs():
    st.subheader("Materialkosten verwalten")
    
    deliveries = load_supplier_deliveries()
    
    edited_df = st.data_editor(
        deliveries,
        column_config={
            "SKU": st.column_config.TextColumn("SKU"),
            "Cost": st.column_config.NumberColumn("Materialkosten", min_value=0, step=0.01),
            "Date": st.column_config.DateColumn("Gültig ab Datum"),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_supplier_deliveries(edited_df)
        st.success("Änderungen wurden gespeichert.")

def get_material_cost_for_date(sku, date):
    deliveries = load_supplier_deliveries()
    relevant_costs = deliveries[(deliveries['SKU'] == sku) & (deliveries['Date'] <= date)]
    if relevant_costs.empty:
        return None
    return relevant_costs.sort_values('Date', ascending=False).iloc[0]['Cost']

def display_overview_table():
    st.subheader("Übersichtstabelle (Letzten 30 Tage)")
    
    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=29)
    
    data = {}
    for single_date in (start_date + timedelta(n) for n in range(30)):
        df = load_from_s3(single_date)
        if df is not None:
            gross_revenue = df['GrossAmount'].sum()
            net_revenue = gross_revenue / 1.19  # Assuming 19% VAT
            material_costs = df.apply(lambda row: get_material_cost_for_date(row['SKU'], single_date) * row['Quantity'], axis=1).sum()
            
            data[single_date] = {
                'Umsatz Brutto': gross_revenue,
                'Umsatz Netto': net_revenue,
                'Materialkosten €': material_costs,
                'Materialkosten %': (material_costs / net_revenue) * 100 if net_revenue > 0 else 0,
                'Deckungsbeitrag 1': net_revenue - material_costs
            }
    
    if data:
        df_overview = pd.DataFrame(data).T
        st.dataframe(df_overview)
    else:
        st.warning("Keine Daten für den ausgewählten Zeitraum verfügbar.")

def main():
    st.title("E-Commerce Profitabilitäts-App")
    
    if st.button("Daten von gestern abrufen"):
        fetch_yesterday_data()
    
    display_overview_table()
    manage_material_costs()

if __name__ == "__main__":
    main()
