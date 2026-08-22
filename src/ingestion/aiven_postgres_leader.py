"""
src/ingestion/aiven_postgres_leader.py
------------------------------------
This script uploads reference data (restaurants, menu items, customers), historical orders, and customer reviews
to an Aiven PostgreSQL database. It reads CSV files from the specified data directory, validates the presence of required environment variables, and writes the data to corresponding tables in the database.
The script uses SQLAlchemy for database connection and Pandas for data manipulation. 
"""

import os
from urllib.parse import quote_plus
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Retrieve connection details and paths from environment variables
HOST = os.getenv("AIVEN_PG_HOST")
PORT = os.getenv("AIVEN_PG_PORT", "17342")
DB_NAME = os.getenv("AIVEN_PG_DB", "defaultdb")
USER = os.getenv("AIVEN_PG_USER", "avnadmin")
PASSWORD = os.getenv("AIVEN_PG_PASSWORD")

# Absolute path to local CSV files
DATA_DIR = r"E:\Data-Engineer-Learning-Path\My-Data-Projects\Finall-Projects-1\Complete_Files\Github-Completed\zaferan_sofreh_pipeline\data"

# Validate critical credentials
if not HOST or not PASSWORD:
    raise ValueError("Missing required environment variables (AIVEN_PG_HOST or AIVEN_PG_PASSWORD) in .env file.")

# URL-encode password to handle special characters safely
encoded_password = quote_plus(PASSWORD)

# Construct SSL Connection String
DATABASE_URL = f"postgresql://{USER}:{encoded_password}@{HOST}:{PORT}/{DB_NAME}?sslmode=require"

# Create Database Engine
engine = create_engine(DATABASE_URL)

# Map local CSV files to PostgreSQL table names
FILES_TO_TABLES = {
    "restaurants.csv": "restaurants",
    "customers.csv": "customers",
    "menu_items.csv": "menu_items",
    "historical_orders.csv": "historical_orders",
    "customer_reviews.csv": "customer_reviews",
}

for csv_file, table_name in FILES_TO_TABLES.items():
    file_path = os.path.join(DATA_DIR, csv_file)
    
    if os.path.exists(file_path):
        print(f"Reading {csv_file} from {file_path}...")
        df = pd.read_csv(file_path)
        
        # Upload to Aiven PostgreSQL (overwrites if table already exists)
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f" Successfully loaded {len(df)} rows into table '{table_name}'")
    else:
        print(f"⚠️ File not found: {file_path}")

print("\n All reference tables have been uploaded to Aiven PostgreSQL!")