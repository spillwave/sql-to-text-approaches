import sys
import os
import logging
import json
from datetime import datetime
from common.visualization_selector import VisualizationSelector, render_visualization

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
from src.common.db_utils import get_db_connection
from src.metadata.get_database_ddl import get_database_ddl
import streamlit as st
import pandas as pd
import json
from sqlalchemy import text
import openai
from typing import Tuple, Dict

def get_db_summary(ddl: str) -> str:
    logger.info("Generating database summary")
    system_prompt = """You are a helpful database expert. Given the database DDL, provide a concise summary of:
    1. The database's main purpose
    2. Key entities/tables
    3. Types/examples of questions users can ask
    Keep the response under 200 words and focus on practical usage."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Here is the database DDL:\n{ddl}"}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
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
    
    system_prompt = """You are an expert SQL query generator. Given a user's question and 
    database DDL, generate a SQL query that answers the question.

    Return your response in the following JSON structure:
    {
        "sql": "the SQL query",
        "explanation": "brief explanation of how the query answers the question",
        "tables_used": ["list", "of", "tables", "used"],
        "expected_result_type": "single_value|list|count|aggregate"
    }

    Guidelines for query generation:
    1. Analyze the question and required tables carefully
    2. Generate a precise SQL query that answers the question
    3. Use appropriate JOINs and WHERE clauses
    4. Keep the query efficient and focused
    5. When asked to return a list of things, reasonably limit the number of results to 10 unless the user has indicated otherwise
    6. When asked to return a count, return the count
    7. When asked to return a single value, return the value
    8. When a table references another table that will add meaningful additional information, perform the join and include the detail
    9. Ensure the SQL syntax is consistent with SQL Server dialect"""

    if error_context:
        system_prompt += f"\n\nPrevious attempt failed with error: {error_context}\nPlease fix the query accordingly."
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Database DDL:\n{ddl}\n\nQuestion: {question}"}
    ]
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.1,
            max_tokens=500,
            response_format={ "type": "json_object" }
        )
        query_response = json.loads(response.choices[0].message.content)
        logger.info(f"Generated SQL query: {query_response['sql']}")
        return query_response
    except Exception as e:
        logger.error(f"Error generating SQL query: {str(e)}")
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
        logger.error(f"Error executing query: {str(e)}")
        raise

def generate_data_interpretation(question: str, query_response: Dict, results_df: pd.DataFrame) -> str:
    logger.info("Generating business interpretation of the data")
    
    # Prepare the context for the LLM
    data_summary = f"Data shape: {results_df.shape[0]} rows, {results_df.shape[1]} columns"
    if not results_df.empty:
        sample_data = results_df.head(3).to_string()
    else:
        sample_data = "No data returned"
    
    system_prompt = """You are a business analyst expert. Given a user's question, SQL query explanation, 
    and the resulting data, provide a concise business-friendly interpretation of what the data shows.
    Focus on key insights and business implications. Keep the response under 100 words."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"""
Question: {question}
Query Explanation: {query_response['explanation']}
Data Summary: {data_summary}
Sample Data:
{sample_data}
        """}
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
        return interpretation
    except Exception as e:
        logger.error(f"Error generating data interpretation: {str(e)}")
        return "Unable to generate data interpretation due to an error."

def add_visualization_options(query_response, results_df):
    # Initialize visualization selector
    viz_selector = VisualizationSelector()

    # Select visualization based on query and data
    viz_config = viz_selector.select_visualization(results_df, query_response["sql"])

    # Create visualization section
    with st.expander("Data Visualization & Interpretation", expanded=True):
        # Generate and display business interpretation
        interpretation = generate_data_interpretation(
            st.session_state.user_question, 
            query_response, 
            results_df
        )
        st.info("**Business Interpretation:**\n" + interpretation)
        
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
st.title("'Plain LLM w/Prompt Chaining' DB Chat Assistant")

# Initialize database schema and summary with a loading spinner
if 'ddl' not in st.session_state or 'db_summary' not in st.session_state:
    with st.spinner('Becoming aware of your database...'):
        logger.info("Initializing database schema and summary")
        if 'ddl' not in st.session_state:
            st.session_state.ddl = get_database_ddl()
            logger.info("Database DDL retrieved")
        if 'db_summary' not in st.session_state:
            st.session_state.db_summary = get_db_summary(st.session_state.ddl)
            logger.info("Database summary generated")

st.info(st.session_state.db_summary)

# User input
user_question = st.text_input("Ask a question about the database:", key="user_question")

if user_question:
    logger.info(f"Processing user question: {user_question}")
    with st.spinner("Processing your question..."):
        # First attempt at query generation and validation
        query_response, is_valid, validation_message = attempt_query_generation_and_validation(user_question, st.session_state.ddl)
        generated_query = query_response["sql"]
        
        # Display query
        with st.expander("Generated SQL Query", expanded=False):
            st.markdown(f"""
            <div class="content-box">
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{generated_query.replace('\n', '<br>')}</pre>
            </div>
            """, unsafe_allow_html=True)
        
        with st.expander("Query Details", expanded=False):
            st.write("**Explanation:**", query_response["explanation"])
            st.write("**Tables Used:**", ", ".join(query_response["tables_used"]))
            st.write("**Expected Result Type:**", query_response["expected_result_type"])
        
        with st.expander("Validation Results", expanded=False):
            st.write("**Valid:**", "✅" if is_valid else "❌")
            st.write("**Validation Message:**", validation_message)
        
        if is_valid:
            st.success("Query validated successfully!")
        else:
            st.warning("Query validation failed, but we'll try to run it anyway")
            
        try:
            with st.spinner("Executing query..."):
                # Attempt query execution with retries
                results_df, execution_success, execution_message = attempt_query_execution(generated_query)
                
                if not execution_success:
                    st.error(execution_message)
                    # Try one more time with error context
                    st.info("Attempting to generate a corrected query...")
                    query_response, is_valid, validation_message = attempt_query_generation_and_validation(
                        user_question, 
                        st.session_state.ddl,
                        validation_retries=1  # Only try one more time
                    )
                    
                    # Display updated validation status
                    if is_valid:
                        st.success("Generated a valid corrected query")
                    else:
                        st.warning("Corrected query validation failed, but we'll try to run it anyway")
                    
                    # Try to execute the query regardless of validation status
                    results_df, execution_success, execution_message = attempt_query_execution(
                        query_response["sql"],
                        max_retries=1  # Only try one more time
                    )
                
                # Handle final results
                if execution_success:
                    st.success("Query executed successfully!")
                    st.dataframe(results_df)
                    add_visualization_options(query_response, results_df)
                else:
                    st.error(f"Final attempt failed: {execution_message}")
                    
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
