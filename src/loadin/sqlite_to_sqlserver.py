import os
import sys
import zipfile
import sqlite3
import pandas as pd
from sqlalchemy import text
from time import sleep
from tqdm import tqdm
from common.db_utils import get_db_connection

def extract_sqlite_file():
    """Extract the SQLite database from zip file"""
    zip_path = 'data/olist.sqlite.zip'
    extract_path = 'data'
    
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"ZIP file not found at {zip_path}")
    
    print(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    
    # Assuming the SQLite file has the same name without .zip
    sqlite_path = os.path.join(extract_path, 'olist.sqlite')
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(f"SQLite file not found at {sqlite_path} after extraction")
    
    return sqlite_path

def get_sqlite_tables(sqlite_path):
    """Get list of all tables in SQLite database"""
    try:
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        conn.close()
        return [table[0] for table in tables]
    except sqlite3.Error as e:
        print(f"Error accessing SQLite database: {e}")
        raise

def wait_for_sql_server(engine, max_attempts=30, delay=2):
    """Wait for SQL Server to be ready"""
    print("Waiting for SQL Server to be ready...")
    for attempt in range(max_attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                print("SQL Server is ready!")
                return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Attempt {attempt + 1}/{max_attempts}: SQL Server not ready yet. Waiting {delay} seconds...")
                sleep(delay)
            else:
                print(f"Could not connect to SQL Server after {max_attempts} attempts: {e}")
                return False

def transfer_data(sqlite_path):
    """Transfer all tables from SQLite to SQL Server"""
    target_db = 'olist'
    
    # First connect to master database with autocommit
    master_engine = get_db_connection(database='master', autocommit=True)
    
    # Wait for SQL Server to be ready
    if not wait_for_sql_server(master_engine):
        raise Exception("SQL Server is not available")
    
    # Create the database if it doesn't exist
    print("Creating database if it doesn't exist...")
    with master_engine.connect() as conn:
        # Check if database exists
        result = conn.execute(text(f"SELECT database_id FROM sys.databases WHERE name = '{target_db}'"))
        if not result.fetchone():
            conn.execute(text(f"CREATE DATABASE {target_db}"))
            print(f"Database '{target_db}' created")
        else:
            print(f"Database '{target_db}' already exists")
    
    # Now connect to our target database
    engine = get_db_connection(database=target_db)
    
    # Connect to SQLite
    try:
        sqlite_conn = sqlite3.connect(sqlite_path)
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        raise
    
    # Get all tables
    tables = get_sqlite_tables(sqlite_path)
    
    print(f"Found {len(tables)} tables to transfer")
    for table in tqdm(tables, desc="Transferring tables", unit="table"):
        try:
            tqdm.write(f"Starting transfer of table: {table}")
            
            # Read data from SQLite
            df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)
            tqdm.write(f"Read {len(df)} rows from {table}")
            
            # Write to SQL Server
            df.to_sql(
                name=table,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            tqdm.write(f"Completed transfer of table: {table}")
        except Exception as e:
            tqdm.write(f"Error transferring table {table}: {e}")
            raise
    
    sqlite_conn.close()
    print("Data transfer completed successfully!")

def main():
    try:
        # Extract SQLite file
        print("Starting data migration process...")
        sqlite_path = extract_sqlite_file()
        
        # Transfer data
        transfer_data(sqlite_path)
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
