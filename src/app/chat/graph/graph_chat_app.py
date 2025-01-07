import sys
import traceback
import os
import logging
import json
from datetime import datetime
from common.visualization_selector import VisualizationSelector, render_visualization
from neo4j import GraphDatabase
from decimal import Decimal

# Get the src directory path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(src_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_chat.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('database_chat')

# Import using absolute paths
from common.db_utils import get_db_connection
from metadata.get_database_ddl import get_database_ddl
import streamlit as st
import pandas as pd
import json
from sqlalchemy import text
import openai
from typing import Tuple, Dict

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Neo4j connection parameters
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

def get_neo4j_driver():
    """Get a Neo4j driver instance"""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def get_graph_summary() -> str:
    """Generate a database summary using graph data"""
    logger.info("Generating database summary from graph")
    with get_neo4j_driver().session() as session:
        # Query to get table and column statistics
        result = session.run("""
        MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
        WITH t.schema as schema, t.name as name, count(c) as column_count
        ORDER BY schema, name
        WITH collect({name: name, schema: schema, columns: column_count}) as tables
        RETURN size(tables) as table_count,
               reduce(total = 0, t IN tables | total + t.columns) as total_columns,
               tables
        """)
        stats = result.single()
        
        # Query to get semantic relationships
        sem_result = session.run("""
        MATCH (c1:Column)-[r:SEMANTIC_RELATION]->(c2:Column)
        WITH type(r) as rel_type, count(r) as rel_count
        RETURN collect({type: rel_type, count: rel_count}) as relationships,
               sum(rel_count) as total_relationships
        """)
        sem_stats = sem_result.single()
        
        # Build summary context
        context = {
            "overview": {
                "table_count": stats["table_count"],
                "total_columns": stats["total_columns"],
                "total_relationships": sem_stats["total_relationships"]
            },
            "tables": stats["tables"],
            "relationships": sem_stats["relationships"]
        }
        
        # Generate summary using LLM
        system_prompt = """You are a helpful database expert. Given the database statistics, provide a concise summary of:
        1. The database's main purpose and structure
        2. Key entities/tables and their relationships
        3. Types of questions users can ask
        Keep the response under 200 words and focus on practical usage."""
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(context, cls=DecimalEncoder)}
        ]
        
        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0.7,
                max_tokens=300
            )
            summary = response.choices[0].message.content
            logger.info("Database summary generated successfully")
            return summary
        except Exception as e:
            logger.error(f"Error generating database summary: {str(e)}")
            raise

def generate_sql_query(question: str, ddl: str, error_context: str = None) -> Dict:
    context_msg = f" with error context: {error_context}" if error_context else ""
    logger.info(f"Generating SQL query for question: {question}{context_msg}")
    
    # First, get relevant context from graph database
    graph_context = generate_cypher_query(question)
    
    system_prompt = """You are an expert SQL query generator. Given a user's question, 
    database DDL, and relevant context from graph analysis, generate a SQL query that answers the question.

    The graph context provides information about which tables and columns are most relevant to the question
    based on semantic relationships and business context. Use this to focus your query on the most relevant
    tables and columns.

    Return your response in the following JSON structure:
    {
        "sql": "the SQL query",
        "explanation": "brief explanation of how the query answers the question",
        "tables_used": ["list", "of", "tables", "used"],
        "expected_result_type": "single_value|list|count|aggregate"
    }

    Guidelines for query generation:
    1. Focus on the tables and columns identified in the graph context
    2. Generate a precise SQL query in SQL Server dialect that answers the question
    3. Use appropriate JOINs and WHERE clauses
    4. Keep the query efficient and focused
    5. When asked to return a list of things, reasonably limit the number of results to 10 unless the user has indicated otherwise. Never use "LIMIT", always use "TOP"
    6. When asked to return a count, return the count
    7. When asked to return a single value, return the value
    8. When a table references another table that will add meaningful additional information, perform the join"""

    if error_context:
        system_prompt += f"\n\nPrevious attempt failed with error: {error_context}\nPlease fix the query accordingly."
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"""Database DDL:\n{ddl}\n
Graph Analysis Context:\n{json.dumps(graph_context, indent=2)}\n
Question: {question}"""}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=500,
            response_format={ "type": "json_object" }
        )
        
        result = json.loads(response.choices[0].message.content)
        result["graph_context"] = graph_context
        return result
        
    except Exception as e:
        logger.error(f"Error generating SQL query: {str(e)}")
        raise

def generate_cypher_query(question: str) -> Dict:
    """Generate a Cypher query to identify relevant tables and columns based on semantic context"""
    logger.info(f"Generating Cypher query for question: {question}")
    
    system_prompt = """You are an expert at generating Cypher queries for Neo4j. Given a user's question, 
    generate a Cypher query that will identify the most relevant tables and columns based on semantic relationships 
    and business context.

    Return your response in the following JSON structure:
    {
        "cypher": "the Cypher query",
        "explanation": "brief explanation of how the query finds relevant context"
    }

    The graph schema has:
    - Table nodes with properties: name, schema
    - Column nodes with properties: name, data_type, description, business_context, synonyms
    - Relationships: 
        - (Table)-[:HAS_COLUMN]->(Column)
        - (Column)-[:SEMANTIC_RELATION {type: 'equivalent|related|derived|component'}]->(Column)
        - (Column)-[:FOREIGN_KEY]->(Column)
        - (Column)-[:PRIMARY_KEY]->(Table)

    Guidelines for writing Cypher queries:
    1. Start with MATCH patterns that find relevant columns based on business_context or synonyms
    2. Use OPTIONAL MATCH for related columns through SEMANTIC_RELATION
    3. Match the table for each column using: MATCH (t:Table)-[:HAS_COLUMN]->(c)
    4. All variables must be introduced in MATCH or OPTIONAL MATCH clauses before being used in WHERE or RETURN
    5. Use WHERE clauses for filtering after establishing patterns
    6. Return distinct results to avoid duplicates
    
    Example query pattern:
    ```
    MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
    WHERE c.business_context CONTAINS $keyword OR c.name CONTAINS $keyword
    OPTIONAL MATCH (c)-[r:SEMANTIC_RELATION]->(related:Column)
    RETURN DISTINCT t.name as table_name, t.schema as schema_name,
           c.name as column_name, c.business_context,
           related.name as related_column_name
    ORDER BY t.name, c.name
    ```
    """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Question: {question}"}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=500,
            response_format={ "type": "json_object" }
        )
        
        result = json.loads(response.choices[0].message.content)
        
        # Execute the Cypher query to get relevant context
        with get_neo4j_driver().session() as session:
            cypher_result = session.run(result["cypher"])
            context = cypher_result.data()
            
            result["context"] = context
            return result
            
    except Exception as e:
        logger.error(f"Error generating Cypher query: {str(e)}")
        raise

def attempt_query_generation_and_validation(question: str, ddl: str, validation_retries: int = 2) -> Tuple[Dict, bool, str]:
    """Attempt to generate and validate a query with retries."""
    current_attempt = 0
    error_context = None
    
    while current_attempt <= validation_retries:
        logger.info(f"Attempt {current_attempt + 1} of {validation_retries + 1} for query generation and validation")
        
        # Generate query
        query_response = generate_sql_query(question, ddl, error_context)
        
        # Validate query
        is_valid, validation_message = validate_query(question, query_response, ddl)
        
        if is_valid:
            logger.info("Query validation successful")
            return query_response, True, validation_message
            
        error_context = f"Validation failed: {validation_message}"
        logger.warning(f"Query validation failed on attempt {current_attempt + 1}: {validation_message}")
        current_attempt += 1
        
        if current_attempt > validation_retries:
            logger.error(f"Query validation failed after {validation_retries} attempts")
            return query_response, False, f"Failed after {validation_retries} validation attempts. Last message: {validation_message}"
    
    return query_response, False, "Maximum validation attempts reached"

def validate_query(question: str, query_response: Dict, ddl: str) -> Tuple[bool, str]:
    logger.info("Validating generated query")
    system_prompt = """You are a SQL query validator. Given a user question, generated SQL query with metadata, and database DDL:
    1. Check if the query will answer the user's question correctly
    2. Verify table relationships and joins are correct
    3. Ensure all necessary conditions are included
    4. Verify the expected result type matches the question intent
    5. Ensure the query reasonably limits the results for lists to less than 20
    
    Return your response in the following JSON structure:
    {
        "is_valid": true/false,
        "explanation": "detailed explanation of validation result",
        "suggested_improvements": ["list", "of", "improvements"] # only if not valid
    }"""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Question: {question}\nQuery Response: {json.dumps(query_response)}\nDDL: {ddl}"}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=300,
            response_format={ "type": "json_object" }
        )
        validation_result = json.loads(response.choices[0].message.content)
        logger.info(f"Query validation result: valid={validation_result['is_valid']}, message={validation_result['explanation']}")
        return validation_result["is_valid"], validation_result["explanation"]
    except Exception as e:
        logger.error(f"Error during query validation: {str(e)}")
        raise

def attempt_query_execution(query: str, max_retries: int = 2) -> Tuple[pd.DataFrame, bool, str]:
    """Attempt to execute a query with retries."""
    current_attempt = 0
    
    while current_attempt <= max_retries:
        logger.info(f"Attempt {current_attempt + 1} of {max_retries + 1} for query execution")
        try:
            results_df = execute_query(query)
            logger.info("Query executed successfully")
            return results_df, True, "Query executed successfully"
        except Exception as e:
            error_message = str(e)
            logger.warning(f"Query execution failed on attempt {current_attempt + 1}: {error_message}")
            current_attempt += 1
            
            if current_attempt > max_retries:
                logger.error(f"Query execution failed after {max_retries} attempts")
                return pd.DataFrame(), False, f"Query execution failed after {max_retries} attempts. Error: {error_message}"
    
    return pd.DataFrame(), False, "Maximum execution attempts reached"

def clean_sql_query(query: str) -> str:
    """Clean a SQL query by removing markdown code blocks and extra whitespace."""
    # Remove markdown code blocks if present
    if query.startswith("```"):
        lines = query.split("\n")
        # Remove first and last lines if they contain ```
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        query = "\n".join(lines)
    
    # Remove extra whitespace
    query = query.strip()
    return query

def execute_query(query: str) -> pd.DataFrame:
    logger.info("Executing SQL query")
    cleaned_query = clean_sql_query(query)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(cleaned_query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error executing query: {str(e)}")
        raise

def generate_data_interpretation(question: str, query_response: Dict, results_df: pd.DataFrame) -> Dict:
    """Generate a business-focused interpretation of the query results."""
    logger.info("Generating data interpretation")
    
    # Convert DataFrame to dict, handling Decimal types
    sample_data = results_df.head(3).to_dict(orient='records')
    sample_data = json.loads(json.dumps(sample_data, cls=DecimalEncoder))
    
    # Prepare context for interpretation
    context = {
        "question": question,
        "row_count": len(results_df),
        "column_count": len(results_df.columns),
        "columns": list(results_df.columns),
        "sample_data": sample_data,
        "graph_context": query_response.get("graph_context", {}),
        "sql_explanation": query_response["explanation"]
    }
    
    system_prompt = """You are a business analyst helping to interpret query results.
    Given the context of the question, the data summary, and the query explanation,
    provide a clear and concise interpretation focused on business insights.
    
    Keep your response focused on what would be most relevant to a business user.
    Highlight key findings, trends, or notable data points."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(context, cls=DecimalEncoder)}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.7,
            max_tokens=150
        )
        interpretation = response.choices[0].message.content
        logger.info("Data interpretation generated successfully")
        return {
            "summary": interpretation,
            "details": {
                "row_count": context["row_count"],
                "columns": context["columns"],
                "graph_context": context["graph_context"]
            }
        }
    except Exception as e:
        logger.error(f"Error generating data interpretation: {str(e)}")
        return {
            "summary": "Unable to generate data interpretation due to an error.",
            "details": {}
        }

def add_visualization_options(query_response, results_df, question: str):
    # Initialize visualization selector
    viz_selector = VisualizationSelector()

    # Select visualization based on query and data
    viz_config = viz_selector.select_visualization(results_df, query_response["sql"])

    # Create visualization section
    with st.expander("Data Visualization & Interpretation", expanded=True):
        # Generate and display business interpretation
        interpretation = generate_data_interpretation(
            question,
            query_response, 
            results_df
        )
        st.info("**Business Interpretation:**\n" + interpretation["summary"])
        
        # Display visualization
        render_visualization(viz_config, results_df, st)
        
        # Show alternative visualization options if confidence is low
        if viz_config['confidence'] < 0.7:
            st.info("Try these alternative visualizations:")
            viz_type = st.selectbox(
                "Select visualization type:",
                ['line', 'bar', 'scatter', 'histogram', 'box', 'pie', 'table']
            )
            
            # Update visualization type and re-render
            if viz_type != viz_config['type']:
                viz_config['type'] = viz_type
                render_visualization(viz_config, results_df, st)


# Streamlit UI
st.title("Graph-Enhanced DB Chat Assistant")

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []

if 'db_summary' not in st.session_state:
    try:
        st.session_state.db_summary = get_graph_summary()
    except Exception as e:
        st.error(f"Error getting database summary: {str(e)}")
        st.session_state.db_summary = "Error loading database summary"

# Display database summary in expandable section
with st.expander("Database Summary", expanded=False):
    st.write(st.session_state.db_summary)

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Get user input
if prompt := st.chat_input("Ask a question about your data"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate assistant response
    with st.chat_message("assistant"):
        try:
            # Get database DDL
            ddl = get_database_ddl()
            
            # Generate and validate query
            query_response, is_valid, error = attempt_query_generation_and_validation(prompt, ddl)
            
            if not is_valid:
                st.error(f"Generated query is invalid: {error}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"I apologize, but I generated an invalid query. Error: {error}"
                })
            else:
                # Show the graph context and Cypher query used
                with st.expander("Graph Analysis", expanded=False):
                    st.json(query_response["graph_context"])
                
                # Show the generated SQL
                with st.expander("Generated SQL", expanded=False):
                    st.code(query_response["sql"], language="sql")
                    st.write("**Explanation:**", query_response["explanation"])
                
                # Execute query and get results
                try:
                    results_df = execute_query(query_response["sql"])
                    
                    # Generate interpretation
                    interpretation = generate_data_interpretation(prompt, query_response, results_df)
                    st.write(interpretation["summary"])
                    
                    # Display results
                    if not results_df.empty:
                        st.dataframe(results_df)
                        
                        # Add visualization options if appropriate
                        add_visualization_options(query_response, results_df, prompt)
                    
                    # Add response to chat history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": interpretation["summary"]
                    })
                    
                except Exception as e:
                    error_msg = f"Error executing query: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg
                    })
        
        except Exception as e:
            error_msg = f"Error processing request: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({
                "role": "assistant",
                "content": error_msg
            })
