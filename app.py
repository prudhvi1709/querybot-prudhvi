# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb",
#     "fastapi",
#     "httpx",
#     "numpy",
#     "pandas",
#     "pydantic",
#     "python-dotenv",
#     "python-multipart",
#     "requests",
#     "uvicorn",
# ]
# ///
import os
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Path
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import shutil
import duckdb
from dotenv import load_dotenv
import io
import csv
from typing import List
import numpy as np
from fastapi.staticfiles import StaticFiles
import httpx
from pathlib import Path
import logging

app = FastAPI()

# Initialize DuckDB and load extensions
con = duckdb.connect(":memory:")
con.execute("INSTALL excel")
con.execute("LOAD excel")
con.execute("INSTALL mysql")
con.execute("LOAD mysql")

SYSTEM_PROMPT = (
    "You are a helpful assistant. Your task is to analyze data from uploaded CSV files. "
    "You will receive the schema for each file and a user query in natural language. "
    "If multiple files are uploaded, observe the schemas of all the files. "
    "Join them only when necessary based on the user query, ensuring that all column names are used exactly as provided in the schema. "
    "Your goal is to convert the query into DuckDB SQL commands, paying close attention to the exact column names from the schema. "
    "Only perform joins if required by the query. After executing the SQL commands, generate insights or results based on the query and schema. "
    "Do not provide code templates or unnecessary explanations.\n\n"
    "For the output, follow this structure:\n"
    "1. Guess the objective of the user based on their query.\n"
    "2. Describe the steps to achieve this objective in SQL.\n"
    "3. Build the logic for the SQL query by identifying the necessary tables and relationships. Select the appropriate columns based on the user's question and the dataset.\n"
    "4. Write SQL to answer the question. Use SQLite syntax.\n"
    "5. Possible Explanation of the query and results."
)

# In-memory storage for uploaded datasets
datasets = {}

# Helper function to call LLM API
async def call_llm_system_prompt(user_input):
    headers = {
        "Authorization": f"Bearer {os.environ['LLMFOUNDRY_TOKEN']}:localdatachat",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_input},
        ],
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://llmfoundry.straive.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30.0  # Added timeout for safety
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

class QueryRequest(BaseModel):
    dataset_name: str
    query: str
    file_path: str

class AnalyzeFileRequest(BaseModel):
    file_path: str  # Now this can contain comma-separated paths

def get_schema_from_duckdb(file_path: str) -> tuple[str, str]:
    """Get schema using DuckDB's introspection capabilities."""
    try:
        file_extension = Path(file_path).suffix.lower()
        con = duckdb.connect(":memory:")

        # Handle different file types
        if file_extension in ['.csv', '.txt']:
            # For CSV files, try to infer schema
            con.execute(f"CREATE TABLE temp AS SELECT * FROM read_csv_auto('{file_path}')")
        elif file_extension == '.parquet':
            con.execute(f"CREATE TABLE temp AS SELECT * FROM parquet_scan('{file_path}')")
        elif file_extension == '.xlsx':
            con.execute(f"CREATE TABLE temp AS SELECT * FROM read_excel('{file_path}')")
        elif file_extension == '.db':
            # For SQLite databases, list all tables and let user choose
            con.execute(f"ATTACH '{file_path}' AS sqlite_db")
            tables = con.execute("SELECT name FROM sqlite_db.sqlite_master WHERE type='table'").fetchall()
            if not tables:
                raise ValueError("No tables found in SQLite database")
            # Use first table for now (could be enhanced to handle multiple tables)
            table_name = tables[0][0]
            con.execute(f"CREATE TABLE temp AS SELECT * FROM sqlite_db.{table_name}")
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        # Get schema information
        schema_info = con.execute("DESCRIBE temp").fetchall()

        # Generate schema description
        schema_description = "CREATE TABLE dataset (\n" + ",\n".join(
            [f"[{col[0]}] {col[1]}" for col in schema_info]
        ) + "\n);"

        # Get sample data for better question suggestions
        sample_data = con.execute("SELECT * FROM temp LIMIT 5").fetchall()

        return schema_description, sample_data

    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise
    finally:
        con.close()

def get_schema_from_mysql(connection_string: str) -> tuple[str, str]:
    """Get schema from MySQL database."""
    con = duckdb.connect(":memory:")
    try:
        # Use DuckDB's MySQL scanner
        con.execute(f"INSTALL mysql")
        con.execute(f"LOAD mysql")
        con.execute(f"CREATE TABLE temp AS SELECT * FROM mysql_scan('{connection_string}')")

        # Get schema and sample data
        schema_info = con.execute("DESCRIBE temp").fetchall()
        sample_data = con.execute("SELECT * FROM temp LIMIT 5").fetchall()

        schema_description = "CREATE TABLE dataset (\n" + ",\n".join(
            [f"[{col[0]}] {col[1]}" for col in schema_info]
        ) + "\n);"

        return schema_description, sample_data
    finally:
        con.close()

@app.post("/upload_csv/")
async def upload_csv(request: AnalyzeFileRequest):
    try:
        # Split the file paths and process each file
        file_paths = [path.strip() for path in request.file_path.split(',')]
        uploaded_datasets = []

        for file_path in file_paths:
            dataset_name = Path(file_path).stem

            # Get schema and sample data using DuckDB
            schema_description, sample_data = get_schema_from_duckdb(file_path)

            # Generate suggested questions using LLM with schema and sample data
            user_prompt = (
                f"Dataset name: {dataset_name}\n"
                f"Schema: {schema_description}\n"
                f"Sample data (first 5 rows): {sample_data}\n"
                "Please provide 5 suggested questions that can be answered using SQL queries on this dataset."
            )
            suggested_questions = await call_llm_system_prompt(user_prompt)

            uploaded_datasets.append({
                "dataset_name": dataset_name,
                "schema": schema_description,
                "suggested_questions": suggested_questions,
                "file_type": Path(file_path).suffix.lower(),
            })

        return {
            "uploaded_datasets": uploaded_datasets
        }

    except Exception as e:
        logger.error(f"Error processing files: {e}")
        return JSONResponse(
            content={"error": f"Error processing files: {str(e)}"},
            status_code=400
        )

@app.post("/query/")
async def query_data(request: QueryRequest):
    try:
        # Split the file paths and process each file
        file_paths = [path.strip() for path in request.file_path.split(',')]

        # Process each file and create tables in DuckDB
        for file_path in file_paths:
            df = pd.read_csv(file_path, encoding='iso-8859-1')
            dataset_name = os.path.splitext(os.path.basename(file_path))[0]

            # Define dtype_mapping for DuckDB
            dtype_mapping = {
                "object": "TEXT",
                "int64": "INTEGER",
                "float64": "FLOAT",
                "bool": "BOOLEAN",
                "datetime64[ns]": "DATETIME",
            }

            # Drop the table if it already exists
            try:
                con.execute(f"DROP TABLE IF EXISTS {dataset_name};")
            except Exception as e:
                return JSONResponse(content={"error": f"Error dropping table: {e}"}, status_code=400)

            # Create table in DuckDB
            con.register("data_table", df)
            con.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM data_table")

            # Generate schema description
            schema_description = f"CREATE TABLE {dataset_name} (\n" + ",\n".join(
                [f"[{col}] {dtype_mapping.get(str(df[col].dtype), 'TEXT')}" for col in df.columns]
            ) + "\n);"

            # Store dataset info
            datasets[dataset_name] = {
                "data": df,
                "schema_description": schema_description
            }

        # Rest of your existing query_data logic
        dataset_schemas = ""
        for name, dataset in datasets.items():
            schema_description = dataset.get("schema_description")
            if schema_description and isinstance(schema_description, str):
                dataset_schemas += f"Dataset name: {name}\nSchema: {schema_description}\n\n"

        # User query
        user_query = request.query

        # Construct LLM prompt
        llm_prompt = (
            f"Here are the datasets available:\n{dataset_schemas}"
            f"Please write an SQL query for the following question:\n{user_query}"
        )

        # Call LLM with the prompt
        llm_response = await call_llm_system_prompt(llm_prompt)

        # Rest of your existing code for executing the query and returning results
        # Extract the SQL query from the response
        import re
        sql_query_match = re.search(r'```sql\n(.*?)\n```', llm_response, re.DOTALL)
        if sql_query_match:
            sql_query = sql_query_match.group(1).strip()
        else:
            return JSONResponse(content={"error": "Failed to extract SQL query from the LLM response."}, status_code=400)

        # Log the extracted SQL query (for debugging)
        print(f"Extracted SQL Query: {sql_query}")

        # Execute the generated SQL query
        result = con.execute(sql_query).fetchdf()

        # Convert any non-JSON serializable types to compatible formats
        result = result.apply(lambda col: col.map(lambda x: x.tolist() if isinstance(x, np.ndarray) else x))
        # Respond with the results
        if isinstance(llm_response, float):
            if llm_response == float('inf') or llm_response == float('-inf') or (isinstance(llm_response, float) and llm_response != llm_response):
                llm_response = None  # or set to 0, depending on your needs
        return JSONResponse(content={
            "result": result.to_dict(orient="records"),
            "generated_query": sql_query,
            "llm_response": llm_response
        })

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=400)

# Mount static files directory LAST
app.mount("/", StaticFiles(directory="static", html=True), name="static")
load_dotenv()
if __name__ == "__main__":
    import uvicorn
    import logging
    logger = logging.getLogger(__name__)
    PORT = int(os.getenv("PORT", 8020))
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except BaseException as e:
        logger.error(f"Running locally. Cannot be accessed from outside: {e}")
        uvicorn.run(app, host="127.0.0.1", port=PORT)
