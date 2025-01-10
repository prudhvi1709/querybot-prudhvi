import os
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Path
from fastapi.responses import JSONResponse, Response
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import shutil
import duckdb
from dotenv import load_dotenv
import io
import csv

app = FastAPI()

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8030"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constant System Prompt
SYSTEM_PROMPT = (
    "You are a helpful assistant. Your task is to analyze data from uploaded CSV files. "
    "You will receive a schema and a user query in natural language queries. "
    "Convert these queries into DuckDB SQL commands that can be executed on the data, "
    "and generate insights or results based on the specific features of the provided schema. Do not provide code templates."
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
async def upload_csv(file: UploadFile = File(...)):
    # Save the uploaded file
    temp_file_path = f"./temp_{file.filename}"
    with open(temp_file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Load CSV into a Pandas DataFrame
    df = pd.read_csv(temp_file_path)
    dataset_name = file.filename.split(".")[0]
    datasets[dataset_name] = df

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

    # Extract schema
    schema = {
        "tables": []
    }
    table_name = dataset_name
    columns = [f"[{col}] {dtype_mapping.get(str(df[col].dtype), 'TEXT')}" for col in df.columns]
    schema["tables"].append(f"CREATE TABLE {table_name} ({', '.join(columns)})")
    schema_description = "\n".join(schema["tables"])

    # Generate suggested questions using LLM
    user_prompt = f"Dataset name: {dataset_name}\nDataset schema:\n{schema_description}\nPlease provide suggested questions."
    suggested_questions = call_llm_system_prompt(user_prompt)

    # Clean up temporary file
    os.remove(temp_file_path)

    # Return the dataset name, schema, and suggested questions immediately
    return {
        "dataset_name": dataset_name,
        "schema": schema,
        "suggested_questions": suggested_questions,
    }
@app.post("/query/")
async def query_data(request: QueryRequest):
    dataset_name = request.dataset_name

    # Check if the dataset exists
    if dataset_name not in datasets:
        return JSONResponse(content={"error": "Dataset not found."}, status_code=404)

    # Validate that the table exists in DuckDB
    try:
        table_exists = con.execute(f"SELECT 1 FROM {dataset_name} LIMIT 1;").fetchall()
    except Exception:
        return JSONResponse(content={"error": f"Table '{dataset_name}' does not exist in DuckDB."}, status_code=404)

    # Generate SQL query dynamically using LLM
    user_query = request.query
    llm_prompt = f"Dataset name: {dataset_name}\nPlease write an SQL query for the following question:\n{user_query}"
    llm_response = call_llm_system_prompt(llm_prompt)

    # Log the LLM response for debugging
    print(f"LLM Response: {llm_response}")

    # Extract the SQL query from the response (strip out markdown and explanation text)
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