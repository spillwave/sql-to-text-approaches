"""
This module handles ingestion of SQL Server schema metadata into Neo4j graph database.
It creates a graph representation of tables, columns, and their relationships.
"""

import os
from neo4j import GraphDatabase
import logging
from metadata.get_database_ddl import get_database_schema
from app.chat.graph.semantic_enrichment import SemanticEnricher

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaGraphBuilder:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password", openai_api_key: str = None):
        """Initialize the Neo4j database connection"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.semantic_enricher = SemanticEnricher(openai_api_key) if openai_api_key else None
        
    def close(self):
        """Close the Neo4j driver connection"""
        self.driver.close()
        
    def clear_graph(self):
        """Clear existing graph data"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def build_schema_graph(self):
        """Build the complete schema graph from database metadata"""
        # Get the schema
        schema = get_database_schema()
        logger.info(f"Retrieved schema with {len(schema['tables'])} tables")
        
        # Clear existing graph
        self.clear_graph()
        logger.info("Cleared existing graph")
        
        # Dictionary to keep track of created nodes
        node_mapping = {}
        
        # First pass: Create all tables and their columns
        logger.info("First pass: Creating tables and columns...")
        for table in schema['tables']:
            # Add table node
            table_node_id = f"{table['schema']}.{table['name']}"
            self.add_table_node(table['name'], table['schema'])
            logger.info(f"Added table node: {table_node_id}")
            
            # Add columns
            for col in table['columns']:
                column_node_id = self.add_column_node(
                    table_node_id,
                    col['name'],
                    col['data_type'],
                    col['is_nullable'],
                    col['description']
                )
                node_mapping[f"{table_node_id}.{col['name']}"] = column_node_id
                logger.info(f"Added column node: {column_node_id}")
            
            # Add primary key relationship if exists
            if table['primary_key']:
                for col_name in table['primary_key']['columns']:
                    col_node_id = node_mapping[f"{table_node_id}.{col_name}"]
                    self.add_primary_key_relationship(
                        col_node_id,
                        table['primary_key']['name']
                    )
                    logger.info(f"Added PK relationship for column: {col_node_id}")

        # Second pass: Create all foreign key relationships
        logger.info("\nSecond pass: Creating foreign key relationships...")
        for table in schema['tables']:
            for fk in table['foreign_keys']:
                for i, col_name in enumerate(fk['columns']):
                    from_col_key = f"{table['schema']}.{table['name']}.{col_name}"
                    to_col_key = f"{fk['references']['schema']}.{fk['references']['table']}.{fk['references']['columns'][i]}"
                    
                    logger.info(f"Creating FK relationship: {from_col_key} -> {to_col_key}")
                    try:
                        self.add_foreign_key_relationship(
                            node_mapping[from_col_key],
                            node_mapping[to_col_key],
                            fk['name']
                        )
                    except KeyError as e:
                        logger.error(f"Could not find column for FK relationship: {e}")
                        logger.error(f"Available columns: {sorted(node_mapping.keys())}")
        
        # Third pass: Add semantic enrichment if enabled
        if self.semantic_enricher:
            logger.info("\nThird pass: Adding semantic enrichment...")
            self.semantic_enricher.enrich_graph(self.driver, schema)

    def add_table_node(self, table_name: str, schema: str) -> str:
        """Add a table node to the graph"""
        node_id = f"{schema}.{table_name}"
        with self.driver.session() as session:
            session.run(
                """
                CREATE (t:Table {
                    id: $node_id,
                    name: $table_name,
                    schema: $schema
                })
                """,
                node_id=node_id,
                table_name=table_name,
                schema=schema
            )
        return node_id
    
    def add_column_node(self, table_node_id: str, column_name: str, 
                       data_type: str, is_nullable: bool, description: str = None) -> str:
        """Add a column node and connect it to its table"""
        column_node_id = f"{table_node_id}.{column_name}"
        with self.driver.session() as session:
            session.run(
                """
                MATCH (t:Table {id: $table_id})
                CREATE (c:Column {
                    id: $column_id,
                    name: $column_name,
                    data_type: $data_type,
                    is_nullable: $is_nullable,
                    description: $description
                })
                CREATE (t)-[:HAS_COLUMN]->(c)
                """,
                table_id=table_node_id,
                column_id=column_node_id,
                column_name=column_name,
                data_type=data_type,
                is_nullable=is_nullable,
                description=description
            )
        return column_node_id

    def add_primary_key_relationship(self, column_id: str, pk_name: str):
        """Add a primary key relationship to a column"""
        with self.driver.session() as session:
            session.run(
                """
                MATCH (c:Column {id: $column_id})
                SET c.is_primary_key = true,
                    c.pk_name = $pk_name
                """,
                column_id=column_id,
                pk_name=pk_name
            )

    def add_foreign_key_relationship(self, from_column_id: str, to_column_id: str, fk_name: str):
        """Add a foreign key relationship between columns"""
        with self.driver.session() as session:
            # First, verify both nodes exist
            result = session.run(
                """
                MATCH (from:Column {id: $from_id})
                MATCH (to:Column {id: $to_id})
                RETURN from, to
                """,
                from_id=from_column_id,
                to_id=to_column_id
            )
            
            if result.single():
                logger.info(f"Found both nodes for FK relationship: {from_column_id} -> {to_column_id}")
                # Create the relationship
                session.run(
                    """
                    MATCH (from:Column {id: $from_id})
                    MATCH (to:Column {id: $to_id})
                    MERGE (from)-[r:REFERENCES {constraint_name: $fk_name}]->(to)
                    RETURN r
                    """,
                    from_id=from_column_id,
                    to_id=to_column_id,
                    fk_name=fk_name
                )
                logger.info(f"Created FK relationship: {from_column_id} -> {to_column_id}")
            else:
                logger.error(f"Could not find one or both nodes for FK relationship: {from_column_id} -> {to_column_id}")

def build_schema_graph(uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password", openai_api_key: str = None):
    """Convenience function to build the schema graph"""
    builder = SchemaGraphBuilder(uri, user, password, openai_api_key)
    try:
        builder.build_schema_graph()
    finally:
        builder.close()

def run_sample_queries(uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
    """Run sample queries to validate graph ingestion"""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            # Count tables
            result = session.run("MATCH (t:Table) RETURN count(t) as table_count")
            print(f"Number of tables: {result.single()['table_count']}")
            
            # Count columns
            result = session.run("MATCH (c:Column) RETURN count(c) as column_count")
            print(f"Number of columns: {result.single()['column_count']}")
            
            # Count primary keys
            result = session.run("MATCH (c:Column) WHERE c.is_primary_key = true RETURN count(c) as pk_count")
            print(f"Number of primary key columns: {result.single()['pk_count']}")
            
            # Count relationships
            result = session.run("MATCH ()-[r:REFERENCES]->() RETURN count(r) as fk_count")
            print(f"Number of foreign key relationships: {result.single()['fk_count']}")
            
            # Sample table and its columns
            result = session.run("""
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WITH t, collect({
                    name: c.name, 
                    type: c.data_type, 
                    is_pk: c.is_primary_key
                }) as columns
                RETURN t.schema + '.' + t.name as table, columns
                LIMIT 1
            """)
            record = result.single()
            print(f"\nSample table: {record['table']}")
            print("Columns:")
            for col in record['columns']:
                pk_marker = " (PK)" if col.get('is_pk') else ""
                print(f"  - {col['name']} {col['type']}{pk_marker}")
            
            # Sample foreign key relationship
            result = session.run("""
                MATCH (from:Column)-[r:REFERENCES]->(to:Column)
                RETURN 
                    split(from.id, '.')[0] + '.' + split(from.id, '.')[1] as from_table,
                    from.name as from_column,
                    split(to.id, '.')[0] + '.' + split(to.id, '.')[1] as to_table,
                    to.name as to_column,
                    r.constraint_name as fk_name
                LIMIT 1
            """)
            record = result.single()
            if record:
                print(f"\nSample foreign key:")
                print(f"From: {record['from_table']}.{record['from_column']}")
                print(f"To: {record['to_table']}.{record['to_column']}")
                print(f"Constraint: {record['fk_name']}")
    finally:
        driver.close()

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"
    openai_api_key = os.environ.get("OPENAI_API_KEY") if os.getenv("OPENAI_API_KEY") else None
    
    print("Building schema graph...")
    build_schema_graph(uri, user, password, openai_api_key)
    print("\nRunning sample queries...")
    run_sample_queries(uri, user, password)
