import os
import re
from sqlalchemy import text
from common.db_utils import get_db_connection
from metadata.get_database_ddl import get_database_ddl
from openai import OpenAI
from typing import Dict, List, Tuple

def get_table_columns(engine, table_name: str) -> List[Tuple[str, str, bool]]:
    """Get columns and their data types for a table"""
    query = """
    SELECT 
        c.name,
        t.name as data_type,
        c.is_nullable,
        COALESCE(ep.value, '') as description
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    LEFT JOIN sys.extended_properties ep ON 
        ep.major_id = c.object_id 
        AND ep.minor_id = c.column_id 
        AND ep.name = 'MS_Description'
    WHERE c.object_id = OBJECT_ID(:table_name)
    ORDER BY c.column_id
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchall()
        return [(row[0], row[1], row[2], row[3]) for row in result]

def get_foreign_key_info(engine, table_name: str) -> List[Dict]:
    """Get foreign key relationships for a table"""
    query = """
    SELECT 
        fk.name as fk_name,
        OBJECT_NAME(fk.parent_object_id) as parent_table,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as parent_column,
        OBJECT_NAME(fk.referenced_object_id) as referenced_table,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as referenced_column
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE OBJECT_NAME(fk.parent_object_id) = :table_name
        OR OBJECT_NAME(fk.referenced_object_id) = :table_name
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"table_name": table_name}).fetchall()
        return [
            {
                "fk_name": row[0],
                "parent_table": row[1],
                "parent_column": row[2],
                "referenced_table": row[3],
                "referenced_column": row[4]
            }
            for row in result
        ]

def generate_column_description(
    table_name: str,
    column_name: str,
    data_type: str,
    is_nullable: bool,
    fk_info: List[Dict],
    existing_description: str
) -> str:
    """Generate detailed column description using OpenAI"""
    
    # Build context about foreign key relationships
    fk_context = []
    for fk in fk_info:
        if fk["parent_table"] == table_name and fk["parent_column"] == column_name:
            fk_context.append(f"This column is a foreign key referencing {fk['referenced_table']}.{fk['referenced_column']}")
        elif fk["referenced_table"] == table_name and fk["referenced_column"] == column_name:
            fk_context.append(f"This column is referenced by {fk['parent_table']}.{fk['parent_column']}")
    
    fk_context_str = "\n".join(fk_context) if fk_context else "This column has no foreign key relationships."
    
    prompt = f"""Analyze this database column and provide a detailed business description:

Table: {table_name}
Column: {column_name}
Data Type: {data_type}
Nullable: {'Yes' if is_nullable else 'No'}
Foreign Key Information:
{fk_context_str}
Existing Description: {existing_description if existing_description else 'None'}

Please provide a comprehensive description that covers:
1. The business purpose and meaning of this column
2. Its relationship to the overall table entity
3. How it connects to other tables (if applicable)
4. Any business rules or constraints implied by its data type and nullability
5. Typical use cases for this column in business analysis

Format the response as a concise paragraph suitable for a SQL column description."""

    try:
        client = OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a database expert who specializes in documenting database schemas with clear, concise, and business-focused descriptions."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error generating description for {table_name}.{column_name}: {str(e)}")
        return existing_description if existing_description else ""

def update_column_description(engine, table_name: str, column_name: str, description: str):
    """Update or add extended property for column description"""
    query = """
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties 
        WHERE major_id = OBJECT_ID(:table_name)
        AND minor_id = (
            SELECT column_id 
            FROM sys.columns 
            WHERE object_id = OBJECT_ID(:table_name) 
            AND name = :column_name
        )
        AND name = 'MS_Description'
    )
    BEGIN
        EXEC sys.sp_updateextendedproperty 
            @name = N'MS_Description',
            @value = :description,
            @level0type = N'SCHEMA',
            @level0name = N'dbo',
            @level1type = N'TABLE',
            @level1name = :table_name,
            @level2type = N'COLUMN',
            @level2name = :column_name
    END
    ELSE
    BEGIN
        EXEC sys.sp_addextendedproperty 
            @name = N'MS_Description',
            @value = :description,
            @level0type = N'SCHEMA',
            @level0name = N'dbo',
            @level1type = N'TABLE',
            @level1name = :table_name,
            @level2type = N'COLUMN',
            @level2name = :column_name
    END
    """
    with engine.connect() as conn:
        conn.execute(text(query), {
            "table_name": table_name,
            "column_name": column_name,
            "description": description
        })
        conn.commit()

def enrich_metadata():
    """Main function to enrich database metadata with column descriptions"""
    # Ensure OpenAI API key is set
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY environment variable must be set")
    
    engine = get_db_connection()
    
    # Get all tables
    table_query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    
    with engine.connect() as conn:
        tables = [row[0] for row in conn.execute(text(table_query)).fetchall()]
    
    # Process each table
    for table_name in tables:
        print(f"\nProcessing table: {table_name}")
        
        # Get foreign key information for context
        fk_info = get_foreign_key_info(engine, table_name)
        
        # Get columns and their current metadata
        columns = get_table_columns(engine, table_name)
        
        # Process each column
        for column_name, data_type, is_nullable, existing_description in columns:
            print(f"  Generating description for column: {column_name}")
            
            # Generate enhanced description
            description = generate_column_description(
                table_name,
                column_name,
                data_type,
                is_nullable,
                fk_info,
                existing_description
            )
            
            # Update the column description in the database
            if description:
                update_column_description(engine, table_name, column_name, description)
                print(f"    Updated description for {column_name}")

if __name__ == "__main__":
    enrich_metadata()
