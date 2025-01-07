# Database Chat Assistant using GraphDB for Metadata

Assume you have followed project root README.md setup instructions.

A Streamlit-based chat application that allows users to query a database using natural language. The app leverages LLM to along with 
Neo4J GraphDB for metadata to generate SQL queries and execute them against the database:

1. Summarize database structure and suggest possible questions
2. Convert natural language questions to SQL queries
3. Validate generated queries for correctness
4. Execute queries and display results

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set your OpenAI API key and enrich the graph with semantic metadata:
```bash
export OPENAI_API_KEY='your-api-key-here'
export PYTHONPATH=./src
python src/app/chat/graph/schema_to_graph.py
```

3. Run the prompt-chaining app:
```bash
export OPENAI_API_KEY='your-api-key-here'
export PYTHONPATH=./src
python -m streamlit run src/app/chat/plain_llm/prompt_chain_app.py
```

## Features

- Database structure summary and suggestion of possible questions
- Natural language to SQL query conversion supported by metadata enrichment and GraphDB analysis 
- Query validation before execution
- Clean display of query results in a table format
- Error handling for invalid queries or execution issues
