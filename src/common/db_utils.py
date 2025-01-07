import urllib.parse
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def wait_for_sql_server(engine, max_attempts=5, delay=2):
    """Wait for SQL Server to be ready"""
    for attempt in range(max_attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Attempt {attempt + 1} failed, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Could not connect to SQL Server after {max_attempts} attempts")
                raise
    return False

def get_db_connection(database='olist', host='localhost', autocommit=False):
    """
    Get database connection properties and create SQLAlchemy engine
    
    Args:
        database (str): Name of the database to connect to. Defaults to 'olist'
        host (str): Database server host. Defaults to 'localhost'
        autocommit (bool): Whether to use AUTOCOMMIT isolation level. Defaults to False
    
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance
    """
    username = 'sa'
    password = 'YourStrong@Passw0rd'
    
    conn_str = (
        'Driver={ODBC Driver 18 for SQL Server};'
        f'Server={host};'
        f'Database={database};'
        f'UID={username};'
        f'PWD={password};'
        'TrustServerCertificate=yes;'
        'Driver=/opt/homebrew/opt/msodbcsql18/lib/libmsodbcsql.18.dylib;'
        'LoginTimeout=30'  # Add explicit login timeout
    )
    
    engine = create_engine(
        f'mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}',
        isolation_level='AUTOCOMMIT' if autocommit else None,
        pool_pre_ping=True  # Add connection health check
    )
    
    # Test the connection
    wait_for_sql_server(engine)
    
    return engine
