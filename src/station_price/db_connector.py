import pyodbc
import pandas as pd
from decouple import config

def get_db_connection():
    """
    Establishes and returns a connection to the SQL database using pyodbc.
    Database credentials are stored in environment variables.
    """
    conn_str = (
        f"DRIVER={config('DB_DRIVER')};"
        f"SERVER={config('DB_SERVER')};"
        f"DATABASE={config('DB_NAME')};"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    return conn


def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch data from the SQL database between the provided start_date and end_date.
    
    Parameters:
    start_date (str): The start date in YYYY-MM-DD format.
    end_date (str): The end date in YYYY-MM-DD format.
    
    Returns:
    pd.DataFrame: A DataFrame containing the query results.
    """
    query = f"""
    SELECT SalesAreaNo, DemoNumber, YearMonth, DurationImpacts, RatecardImpacts
    FROM SalesData
    WHERE TransactionDate BETWEEN '{start_date}' AND '{end_date}'
    """
    query = f"select 1"
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    
    return df