# Database Chat Assistant Using "Plain LLM" with Prompt Chaining

Assume you have followed project root README.md setup instructions.

A Streamlit-based chat application that allows users to query a database using natural language. The app leverages OpenAI's GPT-4 to:
1. Summarize database structure and suggest possible questions
2. Convert natural language questions to SQL queries
3. Validate generated queries for correctness
4. Execute queries and display results

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set your OpenAI API key:
```bash
export OPENAI_API_KEY='your-api-key-here'
```

3. Run the app:
```bash
export OPENAI_API_KEY='your-api-key-here'
export PYTHONPATH=./src
python -m streamlit run src/app/chat/plain_llm/prompt_chain_app.py
```

## Features

- Database structure summary and suggestion of possible questions
- Natural language to SQL query conversion
- Query validation before execution
- Clean display of query results in a table format
- Error handling for invalid queries or execution issues

## Requirements

- Python 3.8+
- OpenAI API key
- SQL Server database connection
- Required Python packages (see requirements.txt, both here and in the parent/root folder)

# Flow
Interrogate the database for metadata
Show the user database details and a list of possible questions
User inputs natural language question
Generate & validate query (up to 2 retries)
Show validation status (success or warning)
Try to execute the query regardless of validation status
If execution fails:
Generate a corrected query
Show validation status
Try to execute the corrected query regardless of validation status
The UI will now show warning messages instead of errors when running unvalidated queries, making it clear that we're still attempting to get results even though the validator has concerns.