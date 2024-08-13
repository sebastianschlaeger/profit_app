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
from src.fulfillment_costs import calculate_shipping_costs, load_fulfillment_costs, save_fulfillment_costs
from src.transaction_costs import load_transaction_costs, save_transaction_costs
from src.marketing_costs import load_marketing_costs, save_marketing_costs


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
            df = fetch_and_process_data(current_date)
            if df is not None:
                all_data.append(df)
            current_date += timedelta(days=1)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            st.success(f"Daten von {start_date} bis {end_date} erfolgreich abgerufen und gespeichert.")
            return combined_df
        else:
            st.warning("Keine Daten für den ausgewählten Zeitraum gefunden.")
            return None
    except Exception as e:
        st.error(f"Fehler beim Abrufen der Daten von {start_date} bis {end_date}. Bitte überprüfen Sie die Logs für weitere Details.")
        return None

def display_overview_page():
    st.subheader("Übersicht anzeigen")
    
    # Initialisiere session_state Variablen
    if 'start_date' not in st.session_state:
        st.session_state.start_date = datetime.now().date() - timedelta(days=7)
    if 'end_date' not in st.session_state:
        st.session_state.end_date = datetime.now().date() - timedelta(days=1)
    if 'show_table' not in st.session_state:
        st.session_state.show_table = False
    if 'selected_marketplace' not in st.session_state:
        st.session_state.selected_marketplace = "Alle"
    if 'selected_country' not in st.session_state:
        st.session_state.selected_country = "Alle"

    # Datumsauswahl
    col1, col2 = st.columns(2)
    with col1:
        st.session_state.start_date = st.date_input("Startdatum", st.session_state.start_date)
    with col2:
        st.session_state.end_date = st.date_input("Enddatum", st.session_state.end_date)

    # Button zum Anzeigen der Tabelle
    if st.button("Übersichtstabelle anzeigen/aktualisieren"):
        st.session_state.show_table = True

    # Wenn die Tabelle angezeigt werden soll
    if st.session_state.show_table:
        display_filtered_overview_table()

def display_filtered_overview_table():
    try:
        all_data = []
        missing_dates = []
        current_date = st.session_state.start_date
        while current_date <= st.session_state.end_date:
            df = load_from_s3(current_date)
            if df is not None and not df.empty:
                all_data.append(df)
            else:
                missing_dates.append(current_date)
            current_date += timedelta(days=1)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Laden der Kosten
            material_costs = load_material_costs()
            fulfillment_costs = load_fulfillment_costs()
            transaction_costs = load_transaction_costs()
            marketing_costs = load_marketing_costs()
            
            # Erstellen der Auswahlfelder für Marktplatz und Land
            unique_marketplaces = ["Alle"] + list(combined_df['Platform'].unique())
            unique_countries = ["Alle"] + list(combined_df['CustomerCountry'].unique())
            
            col1, col2 = st.columns(2)
            with col1:
                st.session_state.selected_marketplace = st.selectbox("Marktplatz auswählen", unique_marketplaces, index=unique_marketplaces.index(st.session_state.selected_marketplace))
            with col2:
                st.session_state.selected_country = st.selectbox("Land auswählen", unique_countries, index=unique_countries.index(st.session_state.selected_country))
            
            # Filtern der Daten basierend auf den Auswahlfeldern
            filtered_df = combined_df
            if st.session_state.selected_marketplace != "Alle":
                filtered_df = filtered_df[filtered_df['Platform'] == st.session_state.selected_marketplace]
            if st.session_state.selected_country != "Alle":
                filtered_df = filtered_df[filtered_df['CustomerCountry'] == st.session_state.selected_country]
            
            if filtered_df.empty:
                st.warning("Keine Daten für die ausgewählten Filter verfügbar.")
            elif material_costs.empty or fulfillment_costs.empty or transaction_costs.empty:
                st.warning("Keine Material-, Fulfillment- oder Transaktionskosten gefunden.")
            else:
                overview_data = calculate_overview_data(filtered_df, material_costs.set_index('SKU')['Cost'].to_dict(), fulfillment_costs, transaction_costs)
                
                # Füge Marketingkosten hinzu
                overview_data = pd.merge(overview_data, marketing_costs, left_on='Datum', right_on='Date', how='left')
                overview_data['Marketingkosten'] = overview_data['Google Ads'] + overview_data['Amazon Ads'] + overview_data['Ebay Ads'] + overview_data['Kaufland Ads']
                overview_data['Marketingkosten'] = overview_data['Marketingkosten'].fillna(0)
                overview_data['Marketingkosten %'] = (overview_data['Marketingkosten'] / overview_data['Umsatz Netto']) * 100
                overview_data['Deckungsbeitrag 3'] = overview_data['Deckungsbeitrag 2'] - overview_data['Marketingkosten']
                
                # Runde die neuen Spalten
                overview_data['Marketingkosten'] = overview_data['Marketingkosten'].round(2)
                overview_data['Marketingkosten %'] = overview_data['Marketingkosten %'].round(1)
                overview_data['Deckungsbeitrag 3'] = overview_data['Deckungsbeitrag 3'].round(2)
                
                # Transponiere die Daten und zeige sie an
                transposed_data = transpose_overview_data(overview_data)
                st.dataframe(transposed_data, height=600, use_container_width=True)
                display_summary(overview_data)
        else:
            st.warning(f"Keine Daten für den ausgewählten Zeitraum verfügbar.")
            if missing_dates:
                st.info(f"Fehlende Daten für folgende Tage: {', '.join(str(date) for date in missing_dates)}")
        
    except Exception as e:
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

def calculate_overview_data(billbee_data, material_costs, fulfillment_costs, transaction_costs, selected_marketplace=None, selected_country=None):
    try:
        billbee_data['CreatedAt'] = pd.to_datetime(billbee_data['CreatedAt']).dt.date
        billbee_data['OrderItems'] = billbee_data['OrderItems'].apply(json.loads)
        
        # Filter nach Marktplatz und Land
        if selected_marketplace:
            billbee_data = billbee_data[billbee_data['Platform'] == selected_marketplace]
        if selected_country:
            billbee_data = billbee_data[billbee_data['CustomerCountry'] == selected_country]
        
        # Berechne Materialkosten
        def calculate_material_cost(order_items):
            return sum(material_costs.get(item['SKU'], 0) * item['Quantity'] for item in order_items)
        
        billbee_data['MaterialCost'] = billbee_data['OrderItems'].apply(calculate_material_cost)
        
        # Berechne Fulfillment-Kosten
        billbee_data['FulfillmentCost'] = (
            fulfillment_costs['Auftragspauschale'].iloc[0] +
            fulfillment_costs['SKU_Pick'].iloc[0] * billbee_data['OrderItems'].apply(lambda x: sum(item['Quantity'] for item in x)) +
            fulfillment_costs['Kartonage'].iloc[0]
        )
        
        # Berechne Versandkosten
        billbee_data['ShippingCost'] = billbee_data.apply(lambda row: calculate_shipping_costs(row['TotalOrderWeight'], row['CustomerCountry']), axis=1)
        
        # Berechne Transaktionskosten
        transaction_cost_dict = dict(zip(transaction_costs['Platform'], transaction_costs['TransactionCostPercent']))
        billbee_data['TransactionCost'] = billbee_data.apply(lambda row: row['TotalOrderPrice'] * transaction_cost_dict.get(row['Platform'], 0) / 100, axis=1)
        
        # Gruppiere die Daten nach Datum
        grouped = billbee_data.groupby('CreatedAt').agg({
            'TotalOrderPrice': 'sum',
            'TaxAmount': 'sum',
            'MaterialCost': 'sum',
            'FulfillmentCost': 'sum',
            'ShippingCost': 'sum',
            'TransactionCost': 'sum'
        }).reset_index()
        
        # Berechne die zusätzlichen Metriken
        grouped['UmsatzNetto'] = grouped['TotalOrderPrice'] - grouped['TaxAmount']
        grouped['MaterialkostenProzent'] = (grouped['MaterialCost'] / grouped['UmsatzNetto']) * 100
        grouped['Deckungsbeitrag1'] = grouped['UmsatzNetto'] - grouped['MaterialCost']
        grouped['GesamtkostenFulfillment'] = grouped['FulfillmentCost'] + grouped['ShippingCost']
        grouped['GesamtkostenFulfillmentProzent'] = (grouped['GesamtkostenFulfillment'] / grouped['UmsatzNetto']) * 100
        grouped['TransaktionskostenProzent'] = (grouped['TransactionCost'] / grouped['UmsatzNetto']) * 100
        grouped['Deckungsbeitrag2'] = grouped['Deckungsbeitrag1'] - grouped['GesamtkostenFulfillment'] - grouped['TransactionCost']
        
        # Formatiere die Tabelle
        result = grouped.rename(columns={
            'CreatedAt': 'Datum',
            'TotalOrderPrice': 'Umsatz Brutto',
            'UmsatzNetto': 'Umsatz Netto',
            'MaterialCost': 'Materialkosten',
            'MaterialkostenProzent': 'Materialkosten %',
            'Deckungsbeitrag1': 'Deckungsbeitrag 1',
            'FulfillmentCost': 'Fulfillment-Kosten',
            'ShippingCost': 'Versandkosten',
            'GesamtkostenFulfillment': 'Gesamtkosten Fulfillment €',
            'GesamtkostenFulfillmentProzent': 'Gesamtkosten Fulfillment %',
            'TransactionCost': 'Transaktionskosten',
            'TransaktionskostenProzent': 'Transaktionskosten %',
            'Deckungsbeitrag2': 'Deckungsbeitrag 2'
        })
        
        # Runde die Zahlen
        for col in ['Umsatz Brutto', 'Umsatz Netto', 'Materialkosten', 'Deckungsbeitrag 1', 
                    'Fulfillment-Kosten', 'Versandkosten', 'Gesamtkosten Fulfillment €', 
                    'Transaktionskosten', 'Deckungsbeitrag 2']:
            result[col] = result[col].round(2)
        for col in ['Materialkosten %', 'Gesamtkosten Fulfillment %', 'Transaktionskosten %']:
            result[col] = result[col].round(1)
        
        return result
    except Exception as e:
        logger.error(f"Fehler bei der Berechnung der Übersichtsdaten: {str(e)}", exc_info=True)
        raise

def transpose_overview_data(overview_data):
    # Entferne die TaxAmount Spalte
    overview_data = overview_data.drop('TaxAmount', axis=1, errors='ignore')
    
    # Berechne Deckungsbeitrag 3 %
    overview_data['Deckungsbeitrag 3 %'] = (overview_data['Deckungsbeitrag 3'] / overview_data['Umsatz Netto']) * 100
    
    # Setze das Datum als Index
    overview_data_indexed = overview_data.set_index('Datum')
    
    # Transponiere die Daten
    transposed_data = overview_data_indexed.transpose()
    
    # Definiere die gewünschte Reihenfolge der Zeilen
    desired_order = [
        'Umsatz Brutto',
        'Umsatz Netto',
        'Materialkosten',
        'Materialkosten %',
        'Deckungsbeitrag 1',
        'Fulfillment-Kosten',
        'Versandkosten',
        'Transaktionskosten',
        'Deckungsbeitrag 2',
        'Marketingkosten',
        'Deckungsbeitrag 3',
        'Deckungsbeitrag 3 %'
    ]
    
    # Sortiere die Daten entsprechend der gewünschten Reihenfolge
    transposed_data = transposed_data.reindex(desired_order)
    
    # Formatiere die Daten
    percentage_rows = ['Materialkosten %', 'Deckungsbeitrag 3 %']
    euro_rows = [row for row in desired_order if row not in percentage_rows]
    
    for row in percentage_rows:
        transposed_data.loc[row] = transposed_data.loc[row].apply(lambda x: f"{x:.0f}%" if pd.notnull(x) else "")
    
    for row in euro_rows:
        transposed_data.loc[row] = transposed_data.loc[row].apply(lambda x: f"{x:.0f} €" if pd.notnull(x) else "")
    
    return transposed_data

def display_summary(overview_data):
    total_gross_revenue = overview_data['Umsatz Brutto'].sum()
    total_net_revenue = overview_data['Umsatz Netto'].sum()
    total_material_cost = overview_data['Materialkosten'].sum()
    total_material_cost_percentage = (total_material_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    total_contribution_margin_1 = overview_data['Deckungsbeitrag 1'].sum()
    total_fulfillment_cost = overview_data['Gesamtkosten Fulfillment €'].sum()
    total_fulfillment_cost_percentage = (total_fulfillment_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    total_transaction_cost = overview_data['Transaktionskosten'].sum()
    total_transaction_cost_percentage = (total_transaction_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    total_contribution_margin_2 = overview_data['Deckungsbeitrag 2'].sum()
    total_marketing_cost = overview_data['Marketingkosten'].sum()
    total_marketing_cost_percentage = (total_marketing_cost / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    total_contribution_margin_3 = overview_data['Deckungsbeitrag 3'].sum()
    
    st.subheader("Zusammenfassung:")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"Umsatz Brutto: {total_gross_revenue:.2f} EUR")
        st.write(f"Umsatz Netto: {total_net_revenue:.2f} EUR")
        st.write(f"Materialkosten: {total_material_cost:.2f} EUR ({total_material_cost_percentage:.1f}%)")
        st.write(f"Deckungsbeitrag 1: {total_contribution_margin_1:.2f} EUR")
        st.write(f"Fulfillment-Kosten: {total_fulfillment_cost:.2f} EUR ({total_fulfillment_cost_percentage:.1f}%)")
    
    with col2:
        st.write(f"Transaktionskosten: {total_transaction_cost:.2f} EUR ({total_transaction_cost_percentage:.1f}%)")
        st.write(f"Deckungsbeitrag 2: {total_contribution_margin_2:.2f} EUR")
        st.write(f"Marketingkosten: {total_marketing_cost:.2f} EUR ({total_marketing_cost_percentage:.1f}%)")
        st.write(f"Deckungsbeitrag 3: {total_contribution_margin_3:.2f} EUR")
    
    # Berechnung der Margen
    db1_margin = (total_contribution_margin_1 / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    db2_margin = (total_contribution_margin_2 / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    db3_margin = (total_contribution_margin_3 / total_net_revenue) * 100 if total_net_revenue != 0 else 0
    
    st.write("---")
    st.write(f"DB1 Marge: {db1_margin:.1f}%")
    st.write(f"DB2 Marge: {db2_margin:.1f}%")
    st.write(f"DB3 Marge: {db3_margin:.1f}%")


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
        st.error(f"Fehler beim Abrufen und Verarbeiten der Daten für {date}. Bitte überprüfen Sie die Logs für weitere Details.")
        return None

def manage_transaction_costs():
    st.subheader("Transaktionskosten verwalten")
    
    costs = load_transaction_costs()
    
    edited_df = st.data_editor(
        costs,
        column_config={
            "Platform": st.column_config.TextColumn("Plattform"),
            "TransactionCostPercent": st.column_config.NumberColumn("Transaktionskosten %", min_value=0, max_value=100, step=0.01),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_transaction_costs(edited_df)
        st.success("Änderungen wurden gespeichert.")


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

def manage_marketing_costs():
    st.subheader("Marketingkosten verwalten")
    
    costs = load_marketing_costs()
    
    edited_df = st.data_editor(
        costs,
        column_config={
            "Date": st.column_config.DateColumn("Datum"),
            "Google Ads": st.column_config.NumberColumn("Google Ads", min_value=0, step=0.01),
            "Amazon Ads": st.column_config.NumberColumn("Amazon Ads", min_value=0, step=0.01),
            "Ebay Ads": st.column_config.NumberColumn("Ebay Ads", min_value=0, step=0.01),
            "Kaufland Ads": st.column_config.NumberColumn("Kaufland Ads", min_value=0, step=0.01),
        },
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_marketing_costs(edited_df)
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
                df = fetch_data_for_range(start_date, end_date)
                if df is not None:
                    st.write(df)
    
    elif main_menu == "Übersicht":
        display_overview_page()
    
    elif main_menu == "Inventory Management":
        inventory_option = st.sidebar.selectbox("Inventory Optionen", [
            "Materialkosten verwalten",
            "Fulfillment-Kosten verwalten",
            "Transaktionskosten verwalten",
            "Marketingkosten verwalten"
        ])
        
        if inventory_option == "Materialkosten verwalten":
            manage_material_costs()
        elif inventory_option == "Fulfillment-Kosten verwalten":
            manage_fulfillment_costs()
        elif inventory_option == "Transaktionskosten verwalten":
            manage_transaction_costs()
        elif inventory_option == "Marketingkosten verwalten":
            manage_marketing_costs()

if __name__ == "__main__":
    main()
