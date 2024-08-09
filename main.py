import streamlit as st
from datetime import datetime, timedelta
import logging
import pandas as pd
import os
import json
from src.billbee_api import BillbeeAPI
from src.s3_operations import save_to_s3, get_saved_dates, load_from_s3, save_daily_order_data
from src.s3_utils import get_s3_fs
from src.data_processor import process_orders, create_dataframe, save_to_csv
from src.fulfillment_costs import load_fulfillment_costs, save_fulfillment_costs


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

def process_order_items(order_items_str):
    items = json.loads(order_items_str.replace("'", '"'))
    return [(item['SKU'], float(item['Quantity'])) for item in items]

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
        missing_dates = []
        current_date = start_date
        while current_date <= end_date:
            df = load_from_s3(current_date)
            if df is not None and not df.empty:
                all_data.append(df)
                logger.info(f"Daten für {current_date} erfolgreich geladen.")
            else:
                missing_dates.append(current_date)
                logger.warning(f"Keine Daten für {current_date} gefunden.")
            current_date += timedelta(days=1)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Gesamtanzahl der geladenen Datensätze: {len(combined_df)}")
            
            material_costs = load_material_costs()
            logger.info(f"Anzahl der geladenen Materialkosten: {len(material_costs)}")
            
            if combined_df.empty:
                st.warning("Die geladenen Daten sind leer.")
                logger.warning("Combined DataFrame ist leer.")
            elif material_costs.empty:
                st.warning("Keine Materialkosten gefunden.")
                logger.warning("Material costs DataFrame ist leer.")
            else:
                overview_data = calculate_overview_data(combined_df, material_costs.set_index('SKU')['Cost'].to_dict())
                
                if overview_data.empty:
                    st.warning("Die berechnete Übersicht ist leer.")
                    logger.warning("Berechnete Übersichtsdaten sind leer.")
                else:
                    st.dataframe(overview_data)
                    display_summary(overview_data)
        else:
            st.warning(f"Keine Daten für den ausgewählten Zeitraum verfügbar.")
            if missing_dates:
                st.info(f"Fehlende Daten für folgende Tage: {', '.join(str(date) for date in missing_dates)}")
            logger.warning(f"Keine Daten für den Zeitraum von {start_date} bis {end_date} gefunden.")
        
        # Zusätzliche Debugging-Informationen
        st.subheader("Debugging-Informationen:")
        st.write(f"Startdatum: {start_date}")
        st.write(f"Enddatum: {end_date}")
        st.write(f"Anzahl der geladenen Datensätze: {sum(len(df) for df in all_data)}")
        st.write(f"Anzahl der Tage ohne Daten: {len(missing_dates)}")
        
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Daten: {str(e)}", exc_info=True)
        st.error(f"Fehler beim Verarbeiten der Daten: {str(e)}")
        st.error("Bitte überprüfen Sie die Logs für weitere Details.")


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

def extract_skus_and_quantities(order_items):
    try:
        if isinstance(order_items, str):
            items = json.loads(order_items)
        elif isinstance(order_items, list):
            items = order_items
        else:
            items = literal_eval(order_items)
        return [(item['Product']['SKU'], item['Quantity']) for item in items]
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von SKUs und Mengen: {str(e)}")
        logger.error(f"Problematische order_items: {order_items}")
        return []

def calculate_overview_data(billbee_data, material_costs):
    try:
        billbee_data['CreatedAt'] = pd.to_datetime(billbee_data['CreatedAt']).dt.date
        billbee_data['OrderItems'] = billbee_data['OrderItems'].apply(process_order_items)
        
        # Berechne Materialkosten
        def calculate_material_cost(order_items):
            return sum(material_costs.get(sku, 0) * quantity for sku, quantity in order_items)
        
        billbee_data['MaterialCost'] = billbee_data['OrderItems'].apply(calculate_material_cost)
        
        # Gruppiere die Daten nach Datum
        grouped = billbee_data.groupby('CreatedAt').agg({
            'TotalOrderPrice': 'sum',
            'TaxAmount': 'sum',
            'MaterialCost': 'sum'
        }).reset_index()
        
        # Berechne die zusätzlichen Metriken
        grouped['UmsatzNetto'] = grouped['TotalOrderPrice'] - grouped['TaxAmount']
        grouped['MaterialkostenProzent'] = (grouped['MaterialCost'] / grouped['UmsatzNetto']) * 100
        grouped['Deckungsbeitrag1'] = grouped['UmsatzNetto'] - grouped['MaterialCost']
        
        # Formatiere die Tabelle
        result = grouped.rename(columns={
            'CreatedAt': 'Datum',
            'TotalOrderPrice': 'Umsatz Brutto',
            'UmsatzNetto': 'Umsatz Netto',
            'MaterialCost': 'Materialkosten',
            'MaterialkostenProzent': 'Materialkosten %',
            'Deckungsbeitrag1': 'Deckungsbeitrag 1'
        })
        
        # Runde die Zahlen
        for col in ['Umsatz Brutto', 'Umsatz Netto', 'Materialkosten', 'Deckungsbeitrag 1']:
            result[col] = result[col].round(2)
        result['Materialkosten %'] = result['Materialkosten %'].round(1)
        
        return result
    except Exception as e:
        logger.error(f"Fehler bei der Berechnung der Übersichtsdaten: {str(e)}", exc_info=True)
        raise
        
def display_summary(overview_data):
    total_gross_revenue = overview_data['Umsatz Brutto'].sum()
    total_net_revenue = overview_data['Umsatz Netto'].sum()
    total_material_cost = overview_data['Materialkosten'].sum()
    total_material_cost_percentage = (total_material_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    total_contribution_margin = overview_data['Deckungsbeitrag 1'].sum()
    
    st.subheader("Zusammenfassung:")
    st.write(f"Umsatz Brutto: {total_gross_revenue:.2f} EUR")
    st.write(f"Umsatz Netto: {total_net_revenue:.2f} EUR")
    st.write(f"Materialkosten: {total_material_cost:.2f} EUR")
    st.write(f"Materialkosten %: {total_material_cost_percentage:.1f}%")
    st.write(f"Deckungsbeitrag 1: {total_contribution_margin:.2f} EUR")

def fetch_and_process_data(date):
    try:
        orders_data = billbee_api.get_orders_for_date(date)
        processed_orders = process_orders(orders_data)
        df = create_dataframe(processed_orders)
        
        filename = f"billbee_orders_{date.strftime('%Y-%m-%d')}.csv"
        save_to_csv(df, filename)
        
        # Save to S3
        s3_path = save_to_s3(df, date)
        
        st.success(f"Daten für {date} erfolgreich abgerufen, verarbeitet und gespeichert.")
        return df
    except Exception as e:
        logger.error(f"Fehler beim Abrufen und Verarbeiten der Daten für {date}: {str(e)}")
        st.error(f"Fehler beim Abrufen und Verarbeiten der Daten für {date}. Bitte überprüfen Sie die Logs für weitere Details.")
        return None

def manage_fulfillment_costs():
    st.subheader("Fulfillment-Kosten verwalten")
    
    costs = load_fulfillment_costs()
    
    edited_df = st.data_editor(
        costs,
        column_config={
            "Auftragspauschale": st.column_config.NumberColumn("Auftragspauschale", min_value=0, step=0.01),
            "SKU_Pick": st.column_config.NumberColumn("SKU Pick", min_value=0, step=0.01),
            "Kartonage": st.column_config.NumberColumn("Kartonage", min_value=0, step=0.01),
            "Versandkosten": st.column_config.NumberColumn("Versandkosten", min_value=0, step=0.01),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_fulfillment_costs(edited_df)
        st.success("Änderungen wurden gespeichert.")

def main():
    st.title("E-Commerce Profitabilitäts-App")
    
    # Sidebar-Menü
    st.sidebar.title("Navigation")
    main_menu = st.sidebar.selectbox("Hauptmenü", ["Daten", "Übersicht", "Inventory Management"])
    
    if main_menu == "Daten":
        data_option = st.sidebar.radio("Daten Optionen", ["Daten von gestern abrufen", "Daten für Zeitraum abrufen"])
        
        if data_option == "Daten von gestern abrufen":
            st.subheader("Daten von gestern abrufen")
            if st.button("Abrufen"):
                yesterday = datetime.now().date() - timedelta(days=1)
                df = fetch_and_process_data(yesterday)
                if df is not None:
                    st.write(df)
        
        elif data_option == "Daten für Zeitraum abrufen":
            st.subheader("Daten für Zeitraum abrufen")
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Startdatum", datetime.now().date() - timedelta(days=7))
            with col2:
                end_date = st.date_input("Enddatum", datetime.now().date() - timedelta(days=1))
            
            if st.button("Daten abrufen"):
                all_data = []
                current_date = start_date
                while current_date <= end_date:
                    df = fetch_and_process_data(current_date)
                    if df is not None:
                        all_data.append(df)
                    current_date += timedelta(days=1)
                
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    st.write(combined_df)
    
    elif main_menu == "Übersicht":
        st.subheader("Übersicht anzeigen")
        start_date = st.date_input("Startdatum", datetime.now().date() - timedelta(days=7))
        end_date = st.date_input("Enddatum", datetime.now().date() - timedelta(days=1))
        if st.button("Übersichtstabelle anzeigen"):
            display_overview_table(start_date, end_date)

    elif main_menu == "Inventory Management":
        inventory_option = st.sidebar.selectbox("Inventory Optionen", ["Materialkosten verwalten", "Fulfillment-Kosten verwalten"])
        
        if inventory_option == "Materialkosten verwalten":
            manage_material_costs()
        elif inventory_option == "Fulfillment-Kosten verwalten":
            manage_fulfillment_costs()
    
    elif main_menu == "Inventory Management":
        inventory_option = st.sidebar.selectbox("Inventory Optionen", ["Materialkosten verwalten"])
        
        if inventory_option == "Materialkosten verwalten":
            st.subheader("Materialkosten verwalten")
            
            material_costs = load_material_costs()
            
            edited_df = st.data_editor(
                material_costs,
                column_config={
                    "SKU": st.column_config.TextColumn("SKU"),
                    "Cost": st.column_config.NumberColumn("Materialkosten", min_value=0, step=0.01),
                },
                num_rows="dynamic"
            )
            
            if st.button("Änderungen speichern"):
                save_material_costs(edited_df)
                st.success("Änderungen wurden gespeichert.")

if __name__ == "__main__":
    main()
