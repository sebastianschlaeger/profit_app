import streamlit as st
import pandas as pd

def display_data_options():
    data_option = st.sidebar.radio("Daten Optionen", ["Daten von gestern abrufen", "Daten für Zeitraum abrufen"])
    return data_option

def display_date_range_input():
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Startdatum", pd.Timestamp.now().date() - pd.Timedelta(days=7))
    with col2:
        end_date = st.date_input("Enddatum", pd.Timestamp.now().date() - pd.Timedelta(days=1))
    return start_date, end_date

def display_overview_table(overview_data):
    st.dataframe(overview_data)

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

def display_cost_editor(costs, column_config, save_function):
    edited_df = st.data_editor(
        costs,
        column_config=column_config,
        num_rows="dynamic"
    )
    
    if st.button("Änderungen speichern"):
        save_function(edited_df)
        st.success("Änderungen wurden gespeichert.")
