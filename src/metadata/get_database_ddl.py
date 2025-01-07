"""
This module provides functions to generate DDL (Data Definition Language) scripts
for SQL Server database objects.
"""

from sqlalchemy import text
from typing import Dict, List, Optional
from common.db_utils import get_db_connection

def get_table_ddl(engine, table_name):
    """Get the DDL for a specific table"""
    query = """
    DECLARE @TableName NVARCHAR(128) = :table_name;
    DECLARE @Result NVARCHAR(MAX) = '';
    
    -- Get column definitions
    SELECT @Result = 'CREATE TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@TableName))) + '.' + QUOTENAME(@TableName) + ' ('
    
    -- Add columns
    SELECT @Result = @Result + CHAR(13) + CHAR(10) + 
        '    ' + QUOTENAME(c.name) + ' ' + 
        CASE WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') 
            THEN t.name + '(' + 
                CASE WHEN c.max_length = -1 
                    THEN 'MAX'
                    ELSE CAST(CASE WHEN t.name LIKE 'n%' 
                        THEN c.max_length/2 
                        ELSE c.max_length END AS VARCHAR(10))
                END + ')'
            WHEN t.name IN ('decimal', 'numeric')
                THEN t.name + '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
            ELSE t.name
        END + ' ' +
        CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END + ','
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(@TableName)
    ORDER BY c.column_id;
    
    -- Add primary key constraint if exists
    SELECT @Result = @Result + CHAR(13) + CHAR(10) + 
        '    CONSTRAINT ' + QUOTENAME(i.name) + ' PRIMARY KEY ' +
        CASE WHEN i.type = 1 THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END +
        ' (' +
        (SELECT STUFF((
            SELECT ', ' + QUOTENAME(c.name) + 
                   CASE WHEN ic.is_descending_key = 1 
                        THEN ' DESC'
                        ELSE ' ASC'
                   END
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE ic.object_id = i.object_id 
                AND ic.index_id = i.index_id
            ORDER BY ic.key_ordinal
            FOR XML PATH('')), 1, 2, '')) + '),'
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(@TableName)
        AND i.is_primary_key = 1;
    
    -- Add unique constraints
    SELECT @Result = @Result + CHAR(13) + CHAR(10) + 
        '    CONSTRAINT ' + QUOTENAME(i.name) + ' UNIQUE ' +
        CASE WHEN i.type = 1 THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END +
        ' (' +
        (SELECT STUFF((
            SELECT ', ' + QUOTENAME(c.name) + 
                   CASE WHEN ic.is_descending_key = 1 
                        THEN ' DESC'
                        ELSE ' ASC'
                   END
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE ic.object_id = i.object_id 
                AND ic.index_id = i.index_id
            ORDER BY ic.key_ordinal
            FOR XML PATH('')), 1, 2, '')) + '),'
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(@TableName)
        AND i.is_unique_constraint = 1;
    
    -- Remove the last comma and close the parentheses
    SET @Result = LEFT(@Result, LEN(@Result) - 1) + CHAR(13) + CHAR(10) + ');'
    
    SELECT @Result;
    """
    ddl_parts = []
    
    # Get the table creation DDL
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchone()
        if result and result[0]:
            ddl_parts.append(result[0])
    
    # Get and add column descriptions
    descriptions = get_column_descriptions(engine, table_name)
    if descriptions:
        ddl_parts.append("\n-- Column Descriptions")
        for column_name, description in descriptions:
            # Escape single quotes in description
            description = description.replace("'", "''")
            ddl_parts.append(f"""EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'{description}',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'{table_name}',
    @level2type = N'COLUMN',
    @level2name = N'{column_name}';""")
    
    return "\n".join(ddl_parts)

def get_column_descriptions(engine, table_name):
    """Get descriptions for all columns in a table"""
    query = """
    SELECT 
        c.name as column_name,
        CAST(ep.value AS NVARCHAR(MAX)) as description
    FROM sys.columns c
    LEFT JOIN sys.extended_properties ep ON 
        ep.major_id = c.object_id 
        AND ep.minor_id = c.column_id 
        AND ep.name = 'MS_Description'
    WHERE c.object_id = OBJECT_ID(:table_name)
        AND ep.value IS NOT NULL
    ORDER BY c.column_id
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchall()
        return [(row[0], row[1]) for row in result]

def get_foreign_key_ddl(engine, table_name):
    """Get the DDL for foreign keys of a specific table"""
    query = """
    SELECT 
        'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + 
        QUOTENAME(OBJECT_NAME(parent_object_id)) + 
        ' ADD CONSTRAINT ' + QUOTENAME(name) + ' FOREIGN KEY (' + 
        (SELECT STUFF((
            SELECT ', ' + QUOTENAME(COL_NAME(fc.parent_object_id, fc.parent_column_id))
            FROM sys.foreign_key_columns fc
            WHERE fc.constraint_object_id = fk.object_id
            ORDER BY fc.constraint_column_id
            FOR XML PATH('')), 1, 2, '')) + 
        ') REFERENCES ' + QUOTENAME(OBJECT_SCHEMA_NAME(referenced_object_id)) + '.' + 
        QUOTENAME(OBJECT_NAME(referenced_object_id)) + ' (' + 
        (SELECT STUFF((
            SELECT ', ' + QUOTENAME(COL_NAME(fc.referenced_object_id, fc.referenced_column_id))
            FROM sys.foreign_key_columns fc
            WHERE fc.constraint_object_id = fk.object_id
            ORDER BY fc.constraint_column_id
            FOR XML PATH('')), 1, 2, '')) + 
        ');'
    FROM sys.foreign_keys fk
    WHERE OBJECT_NAME(parent_object_id) = :table_name
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchall()
        return '\n'.join([row[0] for row in result]) if result else ""

def get_index_ddl(engine, table_name):
    """Get the DDL for indexes of a specific table"""
    query = """
    SELECT
        'CREATE ' + 
        CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END +
        'INDEX ' + QUOTENAME(i.name) + ' ON ' + 
        QUOTENAME(OBJECT_SCHEMA_NAME(i.object_id)) + '.' + 
        QUOTENAME(OBJECT_NAME(i.object_id)) + ' (' +
        (SELECT STUFF((
            SELECT ', ' + QUOTENAME(c.name) + 
                   CASE WHEN ic.is_descending_key = 1 
                        THEN ' DESC'
                        ELSE ' ASC'
                   END
            FROM sys.index_columns ic
            JOIN sys.columns c ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
            WHERE ic.object_id = i.object_id 
                AND ic.index_id = i.index_id
            ORDER BY ic.key_ordinal
            FOR XML PATH('')), 1, 2, '')) + ');'
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(:table_name)
        AND i.type = 2  -- Non-clustered indexes only
        AND i.is_primary_key = 0
        AND i.is_unique_constraint = 0
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchall()
        return '\n'.join([row[0] for row in result]) if result else ""

def get_database_ddl():
    """
    Generate DDL for the entire database
    
    Returns:
        str: Complete DDL script for the database
    """
    engine = get_db_connection()
    ddl_parts = []

    # Get all tables
    table_query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    
    with engine.connect() as conn:
        tables = [row[0] for row in conn.execute(text(table_query)).fetchall()]
    
    # Generate DDL for each table
    for table_name in tables:
        # Table definition
        table_ddl = get_table_ddl(engine, table_name)
        if table_ddl:
            ddl_parts.append(f"-- Table: {table_name}")
            ddl_parts.append(table_ddl)
            ddl_parts.append("\nGO\n")
        
        # Indexes
        index_ddl = get_index_ddl(engine, table_name)
        if index_ddl:
            ddl_parts.append(f"-- Indexes for: {table_name}")
            ddl_parts.append(index_ddl)
            ddl_parts.append("\nGO\n")
    
    # Add foreign keys at the end
    for table_name in tables:
        fk_ddl = get_foreign_key_ddl(engine, table_name)
        if fk_ddl:
            ddl_parts.append(f"-- Foreign Keys for: {table_name}")
            ddl_parts.append(fk_ddl)
            ddl_parts.append("\nGO\n")
    
    return '\n'.join(ddl_parts)

def get_database_schema() -> Dict:
    """
    Get a structured representation of the database schema
    
    Returns:
        Dict: A dictionary containing the complete database schema structure:
        {
            'tables': [
                {
                    'schema': str,
                    'name': str,
                    'columns': [
                        {
                            'name': str,
                            'data_type': str,
                            'is_nullable': bool,
                            'description': Optional[str]
                        }
                    ],
                    'primary_key': {
                        'name': str,
                        'columns': List[str]
                    },
                    'foreign_keys': [
                        {
                            'name': str,
                            'columns': List[str],
                            'references': {
                                'schema': str,
                                'table': str,
                                'columns': List[str]
                            }
                        }
                    ]
                }
            ]
        }
    """
    engine = get_db_connection()
    schema = {'tables': []}
    
    # Get all tables
    table_query = """
    SELECT 
        SCHEMA_NAME(t.schema_id) as schema_name,
        t.name as table_name,
        t.object_id
    FROM sys.tables t
    ORDER BY schema_name, table_name
    """
    
    with engine.connect() as conn:
        tables = conn.execute(text(table_query)).fetchall()
        
        for table in tables:
            table_info = {
                'schema': table.schema_name,
                'name': table.table_name,
                'columns': [],
                'primary_key': None,
                'foreign_keys': []
            }
            
            # Get columns
            column_query = """
            SELECT 
                c.name as column_name,
                t.name as data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                ep.value as description
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.extended_properties ep ON 
                ep.major_id = c.object_id AND 
                ep.minor_id = c.column_id AND 
                ep.name = 'MS_Description'
            WHERE c.object_id = :object_id
            ORDER BY c.column_id
            """
            
            columns = conn.execute(text(column_query), {'object_id': table.object_id}).fetchall()
            for col in columns:
                # Format the data type with precision/scale/length if applicable
                data_type = col.data_type
                if col.data_type in ('char', 'varchar', 'nchar', 'nvarchar'):
                    length = 'MAX' if col.max_length == -1 else str(col.max_length)
                    if col.data_type.startswith('n'):
                        length = str(int(length) // 2) if length != 'MAX' else 'MAX'
                    data_type = f"{col.data_type}({length})"
                elif col.data_type in ('decimal', 'numeric'):
                    data_type = f"{col.data_type}({col.precision},{col.scale})"
                
                table_info['columns'].append({
                    'name': col.column_name,
                    'data_type': data_type,
                    'is_nullable': col.is_nullable,
                    'description': col.description
                })
            
            # Get primary key
            pk_query = """
            SELECT 
                i.name as pk_name,
                c.name as column_name,
                ic.is_descending_key
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = :object_id AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal
            """
            
            pk_columns = conn.execute(text(pk_query), {'object_id': table.object_id}).fetchall()
            if pk_columns:
                table_info['primary_key'] = {
                    'name': pk_columns[0].pk_name,
                    'columns': [col.column_name for col in pk_columns]
                }
            
            # Get foreign keys
            fk_query = """
            SELECT 
                fk.name as fk_name,
                fk_col.name as fk_column_name,
                SCHEMA_NAME(pk_tab.schema_id) as pk_schema_name,
                pk_tab.name as pk_table_name,
                pk_col.name as pk_column_name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fk_cols ON fk.object_id = fk_cols.constraint_object_id
            JOIN sys.columns fk_col ON fk_cols.parent_object_id = fk_col.object_id 
                AND fk_cols.parent_column_id = fk_col.column_id
            JOIN sys.tables pk_tab ON fk.referenced_object_id = pk_tab.object_id
            JOIN sys.columns pk_col ON fk_cols.referenced_object_id = pk_col.object_id 
                AND fk_cols.referenced_column_id = pk_col.column_id
            WHERE fk.parent_object_id = :object_id
            ORDER BY fk.name, fk_cols.constraint_column_id
            """
            
            fk_results = conn.execute(text(fk_query), {'object_id': table.object_id}).fetchall()
            
            # Group foreign key columns by constraint name
            fk_dict = {}
            for fk in fk_results:
                if fk.fk_name not in fk_dict:
                    fk_dict[fk.fk_name] = {
                        'name': fk.fk_name,
                        'columns': [],
                        'references': {
                            'schema': fk.pk_schema_name,
                            'table': fk.pk_table_name,
                            'columns': []
                        }
                    }
                fk_dict[fk.fk_name]['columns'].append(fk.fk_column_name)
                fk_dict[fk.fk_name]['references']['columns'].append(fk.pk_column_name)
            
            table_info['foreign_keys'] = list(fk_dict.values())
            schema['tables'].append(table_info)
    
    return schema

if __name__ == "__main__":
    # Print both DDL and schema for testing
    print("=== DDL Output ===")
    ddl = get_database_ddl()
    print(ddl)
    
    print("\n=== Schema Output ===")
    schema = get_database_schema()
    for table in schema['tables']:
        print(f"\nTable: {table['schema']}.{table['name']}")
        print("Columns:")
        for col in table['columns']:
            print(f"  - {col['name']} {col['data_type']} {'NULL' if col['is_nullable'] else 'NOT NULL'}")
            if col['description']:
                print(f"    Description: {col['description']}")
        if table['primary_key']:
            print("Primary Key:")
            print(f"  {table['primary_key']['name']}: {', '.join(table['primary_key']['columns'])}")
        if table['foreign_keys']:
            print("Foreign Keys:")
            for fk in table['foreign_keys']:
                print(f"  {fk['name']}: ({', '.join(fk['columns'])}) -> "
                      f"{fk['references']['schema']}.{fk['references']['table']}({', '.join(fk['references']['columns'])})")
