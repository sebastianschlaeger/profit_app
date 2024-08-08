import streamlit as st
from datetime import datetime, timedelta
import logging
import pandas as pd
import os
from src.billbee_api import BillbeeAPI
from src.s3_operations import save_to_s3, get_saved_dates, load_from_s3, save_daily_order_data
from src.s3_utils import get_s3_fs

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="E-Commerce Profitabilitäts-App", layout="wide")

# Initialize BillbeeAPI
billbee_api = BillbeeAPI()

def load_and_process_billbee_data(file_path):
    df = pd.read_csv(file_path)
    df['CreatedAt'] = pd.to_datetime(df['CreatedAt']).dt.date
    
    # Gruppieren nach OrderNumber und summieren der relevanten Felder
    grouped = df.groupby('OrderNumber').agg({
        'CreatedAt': 'first',
        'TotalCost': 'sum',
        'ShippingCost': 'sum',
        'TotalPrice': 'sum',
        'TaxAmount': 'sum',
        'Quantity': 'sum'
    }).reset_index()
    
    # Berechnen des Nettoumsatzes
    grouped['NetRevenue'] = grouped['TotalPrice'] - grouped['TaxAmount']
    
    return df, grouped

@st.cache_data
def load_material_costs(file_path='material_costs.csv'):
    if os.path.exists(file_path):
        # Lade die CSV und konvertiere die SKU-Spalte explizit zu Strings
        df = pd.read_csv(file_path)
        df['SKU'] = df['SKU'].astype(str)
        return df
    else:
        st.warning(f"Die Datei {file_path} wurde nicht gefunden. Es wird eine leere Tabelle erstellt.")
        return pd.DataFrame(columns=['SKU', 'Cost'])

def save_material_costs(df, file_path='material_costs.csv'):
    # Stelle sicher, dass SKU als String gespeichert wird
    df['SKU'] = df['SKU'].astype(str)
    df.to_csv(file_path, index=False)
    st.cache_data.clear()  # Cache leeren, damit die Änderungen beim nächsten Laden berücksichtigt werden

def calculate_material_costs(orders_df, material_costs_df):
    # Extrahieren der ersten 5 Ziffern aus der SKU für die Zuordnung
    orders_df['SKU_prefix'] = orders_df['SKU'].str[:5]
    material_costs_df['SKU_prefix'] = material_costs_df['SKU'].str[:5]
    
    # Zusammenführen der Bestellungen mit den Materialkosten
    merged = orders_df.merge(material_costs_df, on='SKU_prefix', how='left')
    
    # Berechnen der Materialkosten pro Bestellung
    merged['MaterialCost'] = merged['Quantity'] * merged['Cost']
    
    return merged

def calculate_profit(processed_df):
    # Gruppieren nach OrderNumber und berechnen der Gesamtmaterialkosten
    order_costs = processed_df.groupby('OrderNumber').agg({
        'MaterialCost': 'sum',
        'NetRevenue': 'first',
        'ShippingCost': 'first'
    }).reset_index()
    
    # Berechnen des Gewinns
    order_costs['Profit'] = order_costs['NetRevenue'] - order_costs['MaterialCost'] - order_costs['ShippingCost']
    
    return order_costs

def fetch_yesterday_data():
    yesterday = datetime.now().date() - timedelta(days=1)
    if yesterday not in get_saved_dates():
        try:
            orders_data = billbee_api.get_orders_for_date(yesterday)
            df = pd.DataFrame(orders_data)
            save_daily_order_data(df, yesterday)
            save_to_s3(df, yesterday)
            st.success(f"Daten für {yesterday} erfolgreich abgerufen und gespeichert.")
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Daten für {yesterday}: {str(e)}")
            st.error(f"Fehler beim Abrufen der Daten für {yesterday}. Bitte überprüfen Sie die Logs für weitere Details.")
    else:
        logger.warning(f"Daten für {yesterday} wurden bereits importiert.")
        st.info(f"Daten für {yesterday} wurden bereits importiert.")

def fetch_data_for_range(start_date, end_date):
    try:
        all_data = []
        current_date = start_date
        while current_date <= end_date:
            orders_data = billbee_api.get_orders_for_date(current_date)
            df = pd.DataFrame(orders_data)
            all_data.append(df)
            save_daily_order_data(df, current_date)
            current_date += timedelta(days=1)
        
        combined_df = pd.concat(all_data, ignore_index=True)
        save_to_s3(combined_df, end_date)
        st.success(f"Daten von {start_date} bis {end_date} erfolgreich abgerufen und gespeichert.")
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Daten von {start_date} bis {end_date}: {str(e)}")
        st.error(f"Fehler beim Abrufen der Daten von {start_date} bis {end_date}. Bitte überprüfen Sie die Logs für weitere Details.")

def display_overview_table(start_date, end_date):
    st.subheader(f"Übersichtstabelle ({start_date} bis {end_date})")
    
    try:
        all_data = []
        current_date = start_date
        while current_date <= end_date:
            df = load_from_s3(current_date)
            if df is not None:
                all_data.append(df)
            current_date += timedelta(days=1)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            material_costs = load_material_costs()
            
            overview_data = calculate_overview_data(combined_df, material_costs)
            
            # Anzeige der Übersichtstabelle
            st.dataframe(overview_data)
            
            # Berechnung und Anzeige der Gesamtwerte
            total_gross_revenue = overview_data['GrossRevenue'].sum()
            total_net_revenue = overview_data['NetRevenue'].sum()
            total_material_cost = overview_data['MaterialCost'].sum()
            total_material_cost_percentage = (total_material_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
            total_contribution_margin = total_net_revenue - total_material_cost
            
            st.subheader("Zusammenfassung:")
            st.write(f"Umsatz Brutto: {total_gross_revenue:.2f} EUR")
            st.write(f"Umsatz Netto: {total_net_revenue:.2f} EUR")
            st.write(f"Materialkosten: {total_material_cost:.2f} EUR")
            st.write(f"Materialkosten %: {total_material_cost_percentage:.2f}%")
            st.write(f"Deckungsbeitrag 1: {total_contribution_margin:.2f} EUR")
        else:
            st.warning("Keine Daten für den ausgewählten Zeitraum verfügbar.")
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Daten: {str(e)}")
        st.error("Fehler beim Verarbeiten der Daten. Bitte überprüfen Sie die Logs für weitere Details.")

def manage_material_costs():
    st.subheader("Materialkosten verwalten")
    
    costs = load_material_costs()
    
    edited_df = st.data_editor(
        costs,
        column_config={
            "SKU": st.column_config.TextColumn("SKU"),
            "Cost": st.column_config.NumberColumn("Materialkosten", min_value=0, step=0.01),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_material_costs(edited_df)
        st.success("Änderungen wurden gespeichert.")

def calculate_overview_data(billbee_data, material_costs):
    # Zusammenführen der Bestelldaten mit den Materialkosten
    merged = billbee_data.merge(material_costs, left_on='SKU', right_on='SKU', how='left')
    
    # Berechnung der relevanten Metriken
    merged['MaterialCost'] = merged['Quantity'] * merged['Cost']
    merged['GrossRevenue'] = merged['TotalPrice']
    merged['NetRevenue'] = merged['TotalPrice'] - merged['TaxAmount']
    
    # Aggregation auf Bestellebene
    order_summary = merged.groupby('OrderNumber').agg({
        'GrossRevenue': 'sum',
        'NetRevenue': 'sum',
        'MaterialCost': 'sum'
    }).reset_index()
    
    # Berechnung der zusätzlichen Metriken
    order_summary['MaterialCostPercentage'] = (order_summary['MaterialCost'] / order_summary['NetRevenue']) * 100
    order_summary['ContributionMargin1'] = order_summary['NetRevenue'] - order_summary['MaterialCost']
    
    return order_summary

def main():
    st.title("E-Commerce Profitabilitäts-App")
    
    # Laden der Materialkosten beim Start der App
    material_costs = load_material_costs()
    
    menu = ["Daten abrufen", "Übersicht anzeigen", "Materialkosten verwalten"]
    choice = st.sidebar.selectbox("Menü", menu)
    
    if choice == "Daten abrufen":
        st.subheader("Daten abrufen")
        if st.button("Daten von gestern abrufen"):
            fetch_yesterday_data()
        
        st.subheader("Daten für Zeitraum abrufen")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Startdatum", datetime.now().date() - timedelta(days=7))
        with col2:
            end_date = st.date_input("Enddatum", datetime.now().date() - timedelta(days=1))
        
        if st.button("Daten für Zeitraum abrufen"):
            fetch_data_for_range(start_date, end_date)
    
    elif choice == "Übersicht anzeigen":
        st.subheader("Übersicht anzeigen")
        start_date = st.date_input("Startdatum", datetime.now().date() - timedelta(days=7))
        end_date = st.date_input("Enddatum", datetime.now().date() - timedelta(days=1))
        if st.button("Übersichtstabelle anzeigen"):
            display_overview_table(start_date, end_date)
    
    elif choice == "Materialkosten verwalten":
        manage_material_costs()

if __name__ == "__main__":
    main()
