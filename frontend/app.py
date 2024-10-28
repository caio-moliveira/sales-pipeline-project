import os
import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st
from ydata_profiling import ProfileReport
from streamlit_pandas_profiling import st_profile_report
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")
st.title("Real-Time Data Visualization")

if st.button('Rerun'):
    st.rerun()


# Load environment variables
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

# Create a PostgreSQL connection string
db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Load data from PostgreSQL "sales_data" table
@st.cache_data(ttl=60)  # Refresh cache every 60 seconds
def load_data() -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM sales_data", connection)
    return df



# KPI Calculation Functions
def calculate_kpis(df: pd.DataFrame):
    total_sales = df['total_value'].sum()
    avg_discount = df['discount'].mean()
    avg_shipping_cost = df['shipping_cost'].mean()
    gross_profit_margin = (df['gross_profit'].sum() / df['total_value'].sum()) * 100
    top_products = df.groupby('product_name')['total_value'].sum().nlargest(5)
    return total_sales, avg_discount, avg_shipping_cost, gross_profit_margin, top_products

# Visualization Functions
def plot_sales_by_category(df: pd.DataFrame):
    sales_by_category = df.groupby('product_category')['total_value'].sum()
    sales_by_category.plot(kind='bar', title='Sales by Product Category')
    plt.xlabel('Product Category')
    plt.ylabel('Total Sales')
    st.pyplot(plt)

def plot_sales_by_region(df: pd.DataFrame):
    sales_by_region = df.groupby('sales_region')['total_value'].sum()
    sales_by_region.plot(kind='bar', title='Sales by Region')
    plt.xlabel('Region')
    plt.ylabel('Total Sales')
    st.pyplot(plt)

def plot_sales_trend(df: pd.DataFrame):
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    sales_trend = df.groupby('sale_date')['total_value'].sum()
    sales_trend.plot(kind='line', title='Sales Trend Over Time')
    plt.xlabel('Date')
    plt.ylabel('Total Sales')
    st.pyplot(plt)

def plot_customer_rating_distribution(df: pd.DataFrame):
    rating_counts = df['customer_rating'].value_counts()
    rating_counts.plot(kind='pie', autopct='%1.1f%%', title='Customer Rating Distribution')
    st.pyplot(plt)

# Main Streamlit app function
def main():
    st.title("Interactive KPI Dashboard")
    st.write("This dashboard loads data from the 'sales_data' table in PostgreSQL and performs KPI analysis.")

    # Load data and display KPIs
    df = load_data()
        
    # Calculate and display KPIs if data exists
    if not df.empty:
        # Calculate KPIs
        total_sales, avg_discount, avg_shipping_cost, gross_profit_margin, top_products = calculate_kpis(df)
        
        # Display KPIs
        st.write("## KPI Overview")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sales", f"${total_sales:,.2f}")
        col2.metric("Average Discount", f"{avg_discount:.2f}%")
        col3.metric("Avg. Shipping Cost", f"${avg_shipping_cost:.2f}")
        col4.metric("Gross Profit Margin", f"{gross_profit_margin:.2f}%")
        
        # Display top products
        st.write("### Top 5 Products by Sales")
        st.table(top_products)

        # Visualization section
        st.write("## Visualizations")

        st.write("### Sales by Product Category")
        plot_sales_by_category(df)

        st.write("### Sales by Region")
        plot_sales_by_region(df)

        st.write("### Sales Trend Over Time")
        plot_sales_trend(df)

        st.write("### Customer Rating Distribution")
        plot_customer_rating_distribution(df)
        
        # Button to generate ProfileReport
        if st.button("Generate Detailed Profile Report", key="profile_report_button"):
            st.write("## Data Profiling Report")
            profile = ProfileReport(df, title="Data Profiling Report", explorative=True)
            st_profile_report(profile)
            
    else:
        st.warning("No data available to display.")



    # Trigger app rerun every 60 seconds
    time.sleep(60)

if __name__ == "__main__":
    main()
