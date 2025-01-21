# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "duckdb",
#     "fastapi",
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
import requests
import shutil
import duckdb
from dotenv import load_dotenv
import io
import csv
from typing import List

app = FastAPI()

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8030"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    "4. Write SQL to answer the question. Use SQLite syntax."
)

# In-memory storage for uploaded datasets
datasets = {}

# Global connection variable
con = duckdb.connect(":memory:")

# Helper function to call LLM API
def call_llm_system_prompt(user_input):
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
    response = requests.post(
        "https://llmfoundry.straive.com/openai/v1/chat/completions",
        headers=headers,
        json=payload,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]

class QueryRequest(BaseModel):
    dataset_name: str
    query: str

@app.post("/upload_csv/")
async def upload_csv(files: List[UploadFile] = File(...)):
    uploaded_datasets = []

    for file in files:
        # Save each uploaded file temporarily
        temp_file_path = f"./temp_{file.filename}"
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Load CSV into a Pandas DataFrame
        df = pd.read_csv(temp_file_path)
        dataset_name = file.filename.split(".")[0]

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
            print(f"Error dropping table: {e}")

        # Create table in DuckDB
        con.register("data_table", df)
        con.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM data_table")
        print(f"Table '{dataset_name}' created in DuckDB.")
        # Generate schema description in proper SQL syntax
        schema_description = f"CREATE TABLE {dataset_name} (\n" + ",\n".join(
            [f"[{col}] {dtype_mapping.get(str(df[col].dtype), 'TEXT')}" for col in df.columns]
        ) + "\n);"

        # Update datasets dictionary
        datasets[dataset_name] = {
            "data": df,
            "schema_description": schema_description
        }
        
        # Generate suggested questions using LLM
        user_prompt = f"Dataset name: {dataset_name}\nDataset schema:\n{schema_description}\nPlease provide suggested questions."
        suggested_questions = call_llm_system_prompt(user_prompt)

        # Clean up temporary file
        os.remove(temp_file_path)

        # Append to the uploaded datasets list
        uploaded_datasets.append({
            "dataset_name": dataset_name,
            "schema": schema_description,
            "suggested_questions": suggested_questions,
        })
        print(f"Tables in memory (upload): {list(datasets.keys())}")
        print("Uploaded datasets:", uploaded_datasets)

    return {"uploaded_datasets": uploaded_datasets}

@app.post("/query/")
async def query_data(request: QueryRequest):
    print(f"Tables in memory (query): {list(datasets.keys())}")
    dataset_name = request.dataset_name

    # Check if the dataset exists
    if dataset_name not in datasets:
        return JSONResponse(content={"error": "Dataset not found."}, status_code=404)

    # Validate that the table exists in DuckDB
    try:
        con.execute(f"SELECT 1 FROM {dataset_name} LIMIT 1;")
    except Exception:
        return JSONResponse(content={"error": f"Table '{dataset_name}' does not exist in DuckDB."}, status_code=404)
    print(f"Table '{dataset_name}' exists in DuckDB.")

    # Constructing a single schema for all datasets
    dataset_schemas = ""
    for name, dataset in datasets.items():
        # Ensure schema_description is added only once per dataset
        if isinstance(dataset.get("schema_description"), str):
            dataset_schemas += f"Dataset name: {name}\nSchema: {dataset['schema_description']}\n\n"

    # User query
    user_query = request.query

    # Construct LLM prompt with all dataset schemas and user query
    llm_prompt = f"Here are the datasets available:\n{dataset_schemas}Please write an SQL query for the following question:\n{user_query}"
    
    # Call LLM with the prompt
    llm_response = call_llm_system_prompt(llm_prompt)
    
    # Log the LLM response for debugging
    print(llm_response)

    # Extract the SQL query from the response
    import re
    sql_query = re.search(r'```sql\n(.*?)\n```', llm_response, re.DOTALL)
    if sql_query:
        sql_query = sql_query.group(1).strip()
    else:
        return JSONResponse(content={"error": "Failed to extract SQL query from the LLM response."}, status_code=400)

    # Log the extracted SQL query (for debugging)
    print(f"Extracted SQL Query: {sql_query}")

    # Execute the generated SQL query
    try:
        result = con.execute(sql_query).fetchdf()
    except Exception as e:
        return JSONResponse(content={"error": f"Failed to execute query: {e}"}, status_code=400)

    # Respond with the results
    return JSONResponse(content={
        "result": result.to_dict(orient="records"),
        "generated_query": sql_query,
        "llm_response": llm_response
    })

@app.get("/download/{table_name:path}")
def download_table(table_name: str = Path(..., title="The name of the table to download")):
    try:
        print(f"Attempting to download table: {table_name}")
        result = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        print(f"Downloaded table: {table_name}")
    except Exception as e:
        return JSONResponse(content={"error": f"Failed to execute query: {e}"}, status_code=400)
    # File path for saving
    file_path = f"{table_name}.csv"
    try:
        # Save the CSV file locally
        result.to_csv(file_path, index=False)
        print(f"File saved locally at {file_path}")
    except Exception as e:
        return JSONResponse(content={"error": f"Failed to save CSV: {e}"}, status_code=500)
    # Serve the file to the client
    return FileResponse(file_path, filename=f"{table_name}.csv", media_type="text/csv")

@app.get("/")
def read_root():
    return {"message": "Welcome to Local DataChat!"}

load_dotenv()

if __name__ == "__main__":
    import uvicorn
    import subprocess
    backend = subprocess.Popen(["uvicorn", "app:app", "--host", "127.0.0.1", "--port", "8020"])
    frontend = subprocess.Popen(["python", "-m", "http.server", "8030"])
    try:
        backend.wait()
        frontend.wait()
    except KeyboardInterrupt:
        backend.terminate()
        frontend.terminate()
