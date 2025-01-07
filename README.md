# Azure AI Project

This project will eventually use Azure AI services, CosmosDB, and other Azure cloud services. For
now its all local dev.

## Setup Instructions

1. Create a Python virtual environment:
Make sure 3.12 is installed and active before you create your venv.
Version verification - 
```shell
brew list | grep python
export PATH="$(brew --prefix)/opt/python@3.12/libexec/bin:$PATH"
```

Ensure you have python 3.12 on your system and selected it in your terminal before running the following:
```bash
python -m venv venv
```

2. Activate the virtual environment:
- On macOS/Linux:
```bash
source venv/bin/activate
```
- On Windows:
```bash
.\venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
docker compose up
```

4. Setup ODBC Driver
Follow all instructions in odbc/README.md

5. Run the ingestion script which takes the sqllite stored data and loads it to sqlserver
```bash
export PYTHONPATH=./src 
./src/loadin/run_data_importer.sh
```

At this point our data has been ingested into the sqlserver database and 
relationships/keys have been created.

6. Make Metadata
Now we want to get the metadata about the tables.

```shell
export PYTHONPATH=./src
python src/metadata/get_database_ddl.py
```

That was mostly informational - now we can use the metadata to generate the
augmented metadata that will drive the graph db schema.

```shell
export OPENAI_API_KEY=YOUR_OPENAI_API_KEY
export PYTHONPATH=./src
python src/metadata/enrich_metadata.py
``` 

7. Extract the Metadata and Use It in Chat

This time when we extract the metadata we should see details about the tables
and columns that will be useful for sql/dax generation.

```shell
export PYTHONPATH=./src
python src/metadata/get_database_ddl.py
```

8. Run the "Plain" LLM Chat

Ok we should be ready to run the SQL Chatbot.
```shell
pip install -r src/app/chat/plain_llm/requirements.txt
export OPENAI_API_KEY='your-api-key-here'
export PYTHONPATH=./src
python -m streamlit run src/app/chat/plain_llm/app.py
```


Extract the Metadata and put it in Graphdb ?? TODO





