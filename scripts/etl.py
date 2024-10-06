import pandas as pd
from sqlalchemy import create_engine

# Database connection string
DB_URI = "postgresql://airflow:airflow@postgres:5432/airflow"

def extract_data(file_path):
    """
    Extract the data from a CSV file.
    """
    print("Extracting data from CSV...")
    df = pd.read_csv(file_path)
    return df

def transform_data(df):
    """
    Perform basic data cleaning and transformation.
    """
    print("Transforming data...")

    # Example transformation: convert start_time and end_time to datetime
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    # Add other transformations here as needed
    return df

def load_data(df, table_name="bergen_bike_data"):
    """
    Load data into PostgreSQL.
    """
    print("Loading data into PostgreSQL...")
    
    # Connect to the PostgreSQL database using SQLAlchemy
    engine = create_engine(DB_URI)
    
    # Load the data into PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print("Data loaded successfully!")

def run_etl(file_path):
    """
    Run the ETL process.
    """
    # Extract
    data = extract_data(file_path)
    
    # Transform
    data = transform_data(data)
    
    # Load
    load_data(data)

if __name__ == "__main__":
    # Example file path for the data
    file_path = "/opt/airflow/data/bergen_merged.csv"
    run_etl(file_path)
