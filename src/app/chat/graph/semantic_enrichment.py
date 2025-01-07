"""
This module handles semantic enrichment of the graph database using LLM analysis
of column metadata to identify related columns and synonyms.
"""

from typing import Dict, List
import logging
import json
from openai import OpenAI
from dataclasses import dataclass

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ColumnSemantics:
    """Data class to hold semantic analysis results for a column"""
    related_columns: List[Dict[str, str]]  # List of {schema: str, table: str, column: str}
    synonyms: List[str]
    business_context: str

class SemanticEnricher:
    def __init__(self, api_key: str):
        """Initialize the semantic enricher with OpenAI API key"""
        self.client = OpenAI(api_key=api_key)
        
    def analyze_column(self, schema: str, table: str, column: str, 
                      data_type: str, description: str) -> ColumnSemantics:
        """
        Analyze a column using LLM to identify related columns and synonyms
        based on its description and context.
        """
        # Construct the prompt for the LLM
        prompt = f"""Given a database column with the following details:
Schema: {schema}
Table: {table}
Column: {column}
Data Type: {data_type}
Description: {description}

Please analyze this column and provide a JSON response with the following structure:
{{
    "related_columns": [
        {{
            "schema": "schema_name",
            "table": "table_name",
            "column": "column_name",
            "relationship_type": "semantic_relationship_type"
        }}
    ],
    "synonyms": ["list", "of", "business", "synonyms"],
    "business_context": "A brief description of the business meaning and usage of this column"
}}

Focus on identifying semantically related columns that might be useful in natural language to SQL translation.
The relationship_type should be one of: equivalent, related, derived, or component.
"""
        
        try:
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a database expert helping to analyze column semantics."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.1,  # Keep it focused and consistent,
                response_format={ "type": "json_object" }
            )
            
            # Parse the response
            result = response.choices[0].message.content
            parsed = json.loads(result)
            
            return ColumnSemantics(
                related_columns=parsed["related_columns"],
                synonyms=parsed["synonyms"],
                business_context=parsed["business_context"]
            )
            
        except Exception as e:
            logger.error(f"Error analyzing column {schema}.{table}.{column}: {str(e)}")
            return ColumnSemantics([], [], "")

    def enrich_graph(self, driver, schema_data: Dict):
        """
        Enrich the Neo4j graph with semantic relationships and metadata
        """
        logger.info("Starting semantic enrichment of graph...")
        
        for table in schema_data['tables']:
            for column in table['columns']:
                # Analyze each column
                semantics = self.analyze_column(
                    table['schema'],
                    table['name'],
                    column['name'],
                    column['data_type'],
                    column.get('description', '')
                )
                
                # Add semantic metadata to the graph
                with driver.session() as session:
                    # Add synonyms and business context
                    session.run(
                        """
                        MATCH (c:Column {id: $column_id})
                        SET c.synonyms = $synonyms,
                            c.business_context = $business_context
                        """,
                        column_id=f"{table['schema']}.{table['name']}.{column['name']}",
                        synonyms=semantics.synonyms,
                        business_context=semantics.business_context
                    )
                    
                    # Add semantic relationships
                    for related in semantics.related_columns:
                        session.run(
                            """
                            MATCH (c1:Column {id: $from_id})
                            MATCH (c2:Column {id: $to_id})
                            MERGE (c1)-[r:SEMANTIC_RELATION {type: $rel_type}]->(c2)
                            """,
                            from_id=f"{table['schema']}.{table['name']}.{column['name']}",
                            to_id=f"{related['schema']}.{related['table']}.{related['column']}",
                            rel_type=related['relationship_type']
                        )
                
                logger.info(f"Enriched column {table['schema']}.{table['name']}.{column['name']}")
