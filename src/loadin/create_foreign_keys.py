import sqlite3
from sqlalchemy import text
import urllib.parse
from tqdm import tqdm
from common.db_utils import get_db_connection

def get_table_columns_with_types(engine, table_name):
    """Get all columns and their data types for a given table"""
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return {row[0]: {'type': row[1], 'length': row[2]} for row in result}

def ensure_valid_key_type(engine, table_name, column_name):
    """Convert column to a valid type for keys if needed"""
    alter_query = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} NVARCHAR(255) NOT NULL;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(alter_query))
            conn.commit()
            return True
    except Exception as e:
        print(f"Error converting column {column_name} in {table_name}: {str(e)}")
        return False

def check_uniqueness(engine, table_name, column_name):
    """Check if a column contains unique values"""
    query = f"""
    SELECT COUNT(*) as total_rows, COUNT(DISTINCT {column_name}) as unique_values
    FROM {table_name}
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            return result[0] == result[1]  # True if all values are unique
    except Exception as e:
        print(f"Error checking uniqueness for {column_name} in {table_name}: {str(e)}")
        return False

def get_primary_key_columns(engine, table_name):
    """Get existing primary key columns for a table"""
    query = f"""
    SELECT c.name as column_name
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.is_primary_key = 1
    AND OBJECT_NAME(i.object_id) = '{table_name}'
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]
    except Exception as e:
        print(f"Error getting primary key for {table_name}: {str(e)}")
        return []

def find_foreign_key_relationships(engine, tables):
    """Find potential foreign key relationships based on column names"""
    relationships = []
    
    for parent_table in tables:
        parent_columns = get_table_columns_with_types(engine, parent_table)
        
        # Look for potential ID columns that might be primary keys
        id_columns = [col for col in parent_columns.keys() if col.lower().endswith('_id')]
        
        for child_table in tables:
            if child_table != parent_table:
                child_columns = get_table_columns_with_types(engine, child_table)
                
                # Find matching columns that could be foreign keys
                for id_col in id_columns:
                    if id_col in child_columns:
                        # Check if this is likely a primary key reference
                        base_name = id_col.lower().replace('_id', '')
                        if parent_table.lower().startswith(base_name):
                            relationships.append({
                                'parent_table': parent_table,
                                'child_table': child_table,
                                'column_name': id_col,
                                'parent_type': parent_columns[id_col],
                                'child_type': child_columns[id_col]
                            })
    
    return relationships

def check_referential_integrity(engine, parent_table, child_table, column_name):
    """Check if all values in child table exist in parent table"""
    query = f"""
    SELECT COUNT(*) as invalid_count
    FROM {child_table} c
    LEFT JOIN {parent_table} p ON c.{column_name} = p.{column_name}
    WHERE p.{column_name} IS NULL
    AND c.{column_name} IS NOT NULL
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            invalid_count = result[0]
            if invalid_count > 0:
                print(f"Warning: Found {invalid_count} rows in {child_table} with {column_name} values that don't exist in {parent_table}")
            return invalid_count == 0
    except Exception as e:
        print(f"Error checking referential integrity between {child_table} and {parent_table}: {str(e)}")
        return False

def create_foreign_keys(engine, relationships):
    """Create foreign key constraints for identified relationships"""
    print("Creating foreign key relationships...")
    
    for rel in tqdm(relationships):
        try:
            parent_table = rel['parent_table']
            child_table = rel['child_table']
            column_name = rel['column_name']
            
            # First ensure the columns are of a valid type for keys
            if not ensure_valid_key_type(engine, parent_table, column_name):
                continue
            if not ensure_valid_key_type(engine, child_table, column_name):
                continue
            
            # Check if parent table already has a primary key
            existing_pk = get_primary_key_columns(engine, parent_table)
            
            # If no primary key exists and the column is unique, create one
            if not existing_pk and check_uniqueness(engine, parent_table, column_name):
                create_pk_query = f"""
                ALTER TABLE {parent_table}
                ADD CONSTRAINT PK_{parent_table} PRIMARY KEY ({column_name});
                """
                with engine.connect() as conn:
                    conn.execute(text(create_pk_query))
                    conn.commit()
                    print(f"Created primary key on {parent_table}({column_name})")
            elif not existing_pk:
                print(f"Warning: Cannot create primary key on {parent_table}({column_name}) - values are not unique")
                continue
            
            # Check referential integrity before creating foreign key
            if not check_referential_integrity(engine, parent_table, child_table, column_name):
                print(f"Skipping foreign key creation due to referential integrity issues")
                continue
            
            # Create foreign key if it doesn't exist
            create_fk_query = f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.foreign_keys
                WHERE parent_object_id = OBJECT_ID('{child_table}')
                AND referenced_object_id = OBJECT_ID('{parent_table}')
            )
            BEGIN
                ALTER TABLE {child_table}
                ADD CONSTRAINT FK_{child_table}_{parent_table}_{column_name}
                FOREIGN KEY ({column_name})
                REFERENCES {parent_table}({column_name});
            END
            """
            
            with engine.connect() as conn:
                conn.execute(text(create_fk_query))
                conn.commit()
                print(f"Successfully created relationship: {child_table}.{column_name} -> {parent_table}.{column_name}")
        except Exception as e:
            print(f"Error creating relationship {rel}: {str(e)}")

def main():
    """Main function to create foreign key relationships"""
    # Create engine for the target database
    engine = get_db_connection()
    
    # Get all tables in the database
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """))
        tables = [row[0] for row in result]
    
    # Find and create foreign key relationships
    relationships = find_foreign_key_relationships(engine, tables)
    if relationships:
        print(f"Found {len(relationships)} potential foreign key relationships")
        create_foreign_keys(engine, relationships)
        print("Foreign key creation completed")
    else:
        print("No potential foreign key relationships found")

if __name__ == "__main__":
    main()
