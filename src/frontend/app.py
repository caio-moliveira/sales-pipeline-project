import os
import re
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import streamlit as st
import altair as alt



st.set_page_config(layout="wide")
st.title("Real-Time Data Visualization")

if st.button('Rerun'):
    st.rerun()

# Load environment variables
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
#POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')

# Create a PostgreSQL connection string
db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres_db/{POSTGRES_DB}"

# Load data from PostgreSQL "sales_data" table
@st.cache_data(ttl=60)  # Refresh cache every 60 seconds
def load_data(retries=5, delay=10):
    engine = create_engine(db_url)
    for attempt in range(retries):
        try:
            with engine.connect() as connection:
                df = pd.read_sql("SELECT * FROM sales_data", connection)
                return df
        except OperationalError:
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                st.warning("Unable to connect to the database after multiple attempts.")
                return pd.DataFrame()


# KPI Calculation Functions
def calculate_kpis(df):
    total_revenue = df['total_value'].sum()
    avg_order_value = df['total_value'].mean()
    total_quantity_sold = df['quantity_sold'].sum()
    avg_quantity_sold = df['quantity_sold'].mean()
    gross_profit_margin = (df['gross_profit'].sum() / total_revenue * 100) if total_revenue != 0 else 0
    return total_revenue, avg_order_value, total_quantity_sold, avg_quantity_sold, gross_profit_margin

# Main Streamlit app function
def main():
    st.title("Interactive Sales KPI Dashboard")
    st.write("Explore sales performance, profitability, and more through varied visualizations.")

    # Load data
    df = load_data()

    # Calculate KPIs
    total_revenue, avg_order_value, total_quantity_sold, avg_quantity_sold, gross_profit_margin = calculate_kpis(df)

    # Display Top KPIs
    st.write("## KPI Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col1.metric("Gross Profit Margin", f"{gross_profit_margin:.2f}%")
    col2.metric("Total Quantity Sold", f"{total_quantity_sold:,.0f}")
    col2.metric("Average Quantity Sold", f"{avg_quantity_sold:.2f}")
    col3.metric("Average Order Value", f"${avg_order_value:,.2f}")

        # Visualization Sections
    st.write("## Revenue and Profitability Insights")

    # Revenue by Product Category using Altair for slimmer bars
    st.write("### Revenue by Product Category")
    revenue_by_category = df.groupby('product_category')['total_value'].sum().reset_index()
    revenue_bar = alt.Chart(revenue_by_category).mark_bar(size=15).encode(
        x=alt.X('total_value:Q', title='Total Revenue'),
        y=alt.Y('product_category:N', sort='-x', title='Product Category')
    ).properties(width=700, height=300)
    st.altair_chart(revenue_bar)

    # Sales Trend Over Time
    st.write("### Sales Trend Over Time")
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    sales_trend = df.groupby('sale_date')['total_value'].sum().reset_index()
    sales_trend_chart = alt.Chart(sales_trend).mark_line().encode(
        x=alt.X('sale_date:T', sort='x', title='Sale Date'),
        y=alt.Y('total_value:Q', title='Total Value')
    ).properties(width=700, height=300)
    st.altair_chart(sales_trend_chart)  

    # Total Value vs Gross Profit by Sales Channel
    st.write("### Total Value vs Gross Profit by Sales Channel")
    scatter_plot = alt.Chart(df).mark_circle(size=60).encode(
        x='total_value',
        y='gross_profit',
        color='sales_channel',
        tooltip=['sales_channel', 'total_value', 'gross_profit']
    ).interactive().properties(width=700, height=400)
    st.altair_chart(scatter_plot)

    # Top 10 Quantity Sold by Sales Representative with slimmer bars
    st.write("### Top 10 Quantity Sold by Sales Representative")
    top_sales_rep = df.groupby('sales_rep')['quantity_sold'].sum().reset_index()
    sales_rep_bar = alt.Chart(top_sales_rep).mark_bar(size=15).encode(
        x=alt.X('quantity_sold:Q', title='Quantity Sold'),
        y=alt.Y('sales_rep:N', sort='-x', title='Sales Representative')
    ).properties(width=700, height=300)
    st.altair_chart(sales_rep_bar)

    # Total Value by Sales Region with slimmer bars
    st.write("### Total Value by Sales Region")
    total_value_by_region = df.groupby('sales_region')['total_value'].sum().reset_index()
    region_bar = alt.Chart(total_value_by_region).mark_bar(size=15).encode(
        x=alt.X('total_value:Q', title='Total Value'),
        y=alt.Y('sales_region:N', sort='-x', title='Sales Region')
    ).properties(width=700, height=300)
    st.altair_chart(region_bar)

    # Interactive Sales Trend by Product Category
    st.write("## Interactive Sales Trend by Product Category")
    selected_category = st.selectbox("Select Product Category", options=df['product_category'].unique())
    filtered_df = df[df['product_category'] == selected_category]
    sales_trend_category = filtered_df.groupby('sale_date')['total_value'].sum().reset_index()
    category_trend_chart = alt.Chart(sales_trend_category).mark_line().encode(
        x=alt.X('sale_date:T', sort='x', title='Sale Date'),
        y=alt.Y('total_value:Q', title='Total Value')
    ).properties(width=700, height=300)
    st.altair_chart(category_trend_chart)

if __name__ == "__main__":
    main()