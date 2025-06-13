from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from platformdirs import user_config_dir
from pydantic import BaseModel
from typing import List
import duckdb
import httpx
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import math

# Custom JSON encoder to handle non-serializable values
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Series):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return super().default(obj)

load_dotenv()

config_dir = user_config_dir("dataquery")
app = FastAPI()

# Use custom JSON encoder for all responses
app.json_encoder = CustomJSONEncoder

# Initialize DuckDB and load extensions
con = duckdb.connect(":memory:")
con.execute("INSTALL excel")
con.execute("LOAD excel")
con.execute("INSTALL mysql")
con.execute("LOAD mysql")

SYSTEM_PROMPT = (
    "You are an expert data analyst tasked with analyzing data using DuckDB SQL syntax. "
    "Based on the user's question, determine the appropriate analytical approach:\n\n"
    "For questions about complaints/issues/problems:\n"
    "- Use a simple but effective approach with LIKE operators\n"
    "- Example query structure:\n"
    "  SELECT ReviewText, COUNT(*) as Frequency\n"
    "  FROM dataset\n"
    "  WHERE LOWER(ReviewText) LIKE '%keyword1%'\n"
    "     OR LOWER(ReviewText) LIKE '%keyword2%'\n"
    "  GROUP BY ReviewText\n"
    "  ORDER BY Frequency DESC\n"
    "For trends or patterns:\n"
    "- Use simple aggregations (COUNT, AVG, SUM)\n"
    "- Group by relevant columns\n"
    "- Always use LOWER() for case-insensitive text matching\n\n"
    "For comparative analysis:\n"
    "- Use simple subqueries or window functions\n"
    "- Include HAVING clauses for filtered aggregations\n"
    "- Sort results meaningfully\n\n"
    "Important rules:\n"
    "1. Always use simple, DuckDB-compatible SQL syntax\n"
    "2. Avoid complex string manipulations\n"
    "3. Use straightforward GROUP BY and aggregations\n"
    "4. Never use LIMIT, Display all results\n"
    "5. Focus on finding meaningful patterns in the data\n\n"
    "When working with dates in DuckDB:\n"
    "1. Never use DATE() function directly on columns\n"
    "2. Do NOT use julianday() function (it doesn't exist in DuckDB)\n"
    "3. For date conversions, use STRPTIME with multiple formats to handle various date inputs:\n"
    "   - STRPTIME(column_name, ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y', '%m-%d-%Y', '%d.%m.%Y', '%Y.%m.%d'])\n"
    "   - This will automatically try each format and convert to ISO 8601 (YYYY-MM-DD)\n"
    "4. For date differences, use DATE_DIFF('day', date1, date2) function\n"
    "5. For date comparisons, use the BETWEEN operator or simple comparison operators\n"
    "6. For date operations, use date_sub(), date_add() functions\n\n"
    "For the output, follow this structure:\n"
    "1. Guess the objective of the user based on their query.\n"
    "2. Describe the steps to achieve this objective in SQL.\n"
    "3. Build the logic for the SQL query by identifying the necessary tables and relationships. Select the appropriate columns based on the user's question and the dataset.\n"
    "4. Write SQL to answer the question. Use DuckDB-compatible syntax.\n"
    "5. Possible Explanation of the query and results.\n\n"
    "Always ensure queries are compatible with DuckDB and provide clear insights."
)

# In-memory storage for uploaded datasets
datasets = {}


# Helper function to call LLM API
async def call_llm_system_prompt(user_input, model="gpt-4.1-nano", api_base=None):
    # Use the system prompt directly, no need to check for date handling
    current_prompt = SYSTEM_PROMPT
    
    headers = {
        "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}:querybot",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": current_prompt},
            {"role": "user", "content": user_input},
        ],
    }
    
    # Use custom API base URL if provided, otherwise use the default
    base_url = api_base if api_base else os.environ['OPENAI_API_BASE']
    
    async with httpx.AsyncClient() as client:
        # Check if the base URL already ends with /chat/completions
        url = base_url if base_url.endswith("/chat/completions") else f"{base_url}/chat/completions"
        response = await client.post(
            url,
            headers=headers,
            json=payload,
            timeout=30.0,  # Added timeout for safety
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


class QueryRequest(BaseModel):
    dataset_name: str
    query: str
    file_path: str
    is_explanation: bool = False
    system_prompt: str | None = None  # Make system_prompt optional
    model: str = "gpt-4.1-nano"  # Default model
    api_base: str | None = None  # Optional custom API base URL

class AnalyzeFileRequest(BaseModel):
    file_paths: List[str]  # Now this can contain comma-separated paths


def get_schema_from_duckdb(file_path: str) -> tuple[str, str]:
    """Get schema using DuckDB's introspection capabilities."""
    con = None  # Initialize con to None
    try:
        if file_path.startswith('http://') or file_path.startswith('https://'):
            # Handle URL
            try:
                con = duckdb.connect(file_path, read_only=True)
            except Exception as e:
                logging.error(f"Error connecting to DuckDB URL {file_path}: {e}")
                raise ValueError(f"Could not connect to DuckDB URL: {file_path}") from e

            tables = con.execute("SHOW TABLES;").fetchall()
            if not tables:
                raise ValueError("No tables found in the online DuckDB database")

            table_name = tables[0][0] # Use the first table found

            # Get schema information
            schema_info = con.execute(f"DESCRIBE {table_name};").fetchall()

            # Generate schema description
            schema_description = (
                f"CREATE TABLE {table_name} (\n"
                + ",\n".join([f"[{col[0]}] {col[1]}" for col in schema_info])
                + "\n);"
            )

            # Get sample data
            sample_data = con.execute(f"SELECT * FROM {table_name} LIMIT 5;").fetchall()

            return schema_description, sample_data
        else:
            # Handle local file
            file_extension = Path(file_path).suffix.lower()
            con = duckdb.connect(":memory:")

            # Handle different file types
            if file_extension in [".csv", ".txt"]:
                con.execute(f"CREATE TABLE temp AS SELECT * FROM read_csv_auto('{file_path}')")
            elif file_extension == ".parquet":
                con.execute(f"CREATE TABLE temp AS SELECT * FROM parquet_scan('{file_path}')")
            elif file_extension == ".xlsx":
                con.execute(f"CREATE TABLE temp AS SELECT * FROM read_excel('{file_path}')")
            elif file_extension == ".db":
                con.execute(f"ATTACH '{file_path}' AS sqlite_db")
                tables = con.execute(
                    "SELECT name FROM sqlite_db.sqlite_master WHERE type='table'"
                ).fetchall()
                if not tables:
                    raise ValueError("No tables found in SQLite database")
                table_name = tables[0][0]
                con.execute(f"CREATE TABLE temp AS SELECT * FROM sqlite_db.{table_name}")
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")

            # Get schema information
            schema_info = con.execute("DESCRIBE temp").fetchall()

            # Generate schema description
            schema_description = (
                "CREATE TABLE dataset (\n"
                + ",\n".join([f"[{col[0]}] {col[1]}" for col in schema_info])
                + "\n);"
            )

            # Get sample data for better question suggestions
            sample_data = con.execute("SELECT * FROM temp LIMIT 5").fetchall()

            return schema_description, sample_data

    except Exception as e:
        logging.error(f"Error processing file/URL {file_path}: {e}")
        raise
    finally:
        if con:
            con.close()


def get_schema_from_mysql(connection_string: str) -> tuple[str, str]:
    """Get schema from MySQL database."""
    con = duckdb.connect(":memory:")
    try:
        # Use DuckDB's MySQL scanner
        con.execute("INSTALL mysql")
        con.execute("LOAD mysql")
        con.execute(f"CREATE TABLE temp AS SELECT * FROM mysql_scan('{connection_string}')")

        # Get schema and sample data
        schema_info = con.execute("DESCRIBE temp").fetchall()
        sample_data = con.execute("SELECT * FROM temp LIMIT 5").fetchall()

        schema_description = (
            "CREATE TABLE dataset (\n"
            + ",\n".join([f"[{col[0]}] {col[1]}" for col in schema_info])
            + "\n);"
        )

        return schema_description, sample_data
    finally:
        con.close()


@app.get("/list-files")
async def list_files():
    return {"files": [f.name for f in Path(config_dir).glob("*.csv")]}


@app.post("/upload")
async def upload_csv(request: AnalyzeFileRequest):
    # Split the file paths and process each file
    uploaded_datasets = []

    for file_path in request.file_paths:
        dataset_name = Path(file_path).stem

        # Get schema and sample data using DuckDB
        schema_description, _ = get_schema_from_duckdb(file_path)

        # Generate suggested questions using LLM with schema and sample data
        user_prompt = (
            f"Dataset name: {dataset_name}\n"
            f"Schema: {schema_description}\n"
            "Please provide 5 suggested questions (ONLY QUESTIONS, NO EXPLANATION, NO Serial Numbers) that can be answered using duckDB queries on this dataset."
        )
        suggested_questions = await call_llm_system_prompt(user_prompt, "gpt-4.1-nano")

        uploaded_datasets.append({
                "dataset_name": dataset_name,
                "schema": schema_description,
                "suggested_questions": suggested_questions,
                "file_type": Path(file_path).suffix.lower(),
            })

    return {"uploaded_datasets": uploaded_datasets}


@app.post("/query")
async def query_data(request: QueryRequest):
    try:
        # Handle explanation requests differently
        if request.is_explanation:
            # Use the system prompt provided in the request for explanations
            system_prompt = request.system_prompt if request.system_prompt else SYSTEM_PROMPT

            # Create messages with the appropriate system prompt
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": request.query},
            ]

            # Call LLM with the specific system prompt
            headers = {
                "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}:querybot",
            }
            payload = {
                "model": request.model,
                "messages": messages,
            }

            # Use custom API base URL if provided, otherwise use the default
            api_base = request.api_base if request.api_base else os.environ['OPENAI_API_BASE']
            
            async with httpx.AsyncClient() as client:
                # Check if the base URL already ends with /chat/completions
                url = api_base if api_base.endswith("/chat/completions") else f"{api_base}/chat/completions"
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=30.0,
                )
                response.raise_for_status()
                llm_response = response.json()["choices"][0]["message"]["content"]

            return JSONResponse(content={
                "llm_response": llm_response
            })

        # Split the file paths and process each file
        file_paths = [path.strip() for path in request.file_path.split(",")]
        online_db_alias_counter = 0
        datasets.clear() # Clear previous datasets state

        # Define dtype_mapping for DuckDB
        dtype_mapping = {
            "object": "TEXT",
            "int64": "INTEGER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "DATETIME",
        }

        # Process each file path
        for file_path in file_paths:
            if file_path.startswith('http://') or file_path.startswith('https://'):
                # Handle online DuckDB URL
                alias = f"online_db_{online_db_alias_counter}"
                online_db_alias_counter += 1
                try:
                    con.execute(f"ATTACH '{file_path}' AS {alias} (READ_ONLY);")
                    # Fetch tables from the attached database
                    tables_in_attached_db = con.execute(
                        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{alias}'"
                    ).fetchall()

                    if not tables_in_attached_db:
                        logging.warning(f"No tables found in online database {file_path} with alias {alias}")
                        continue

                    for (tbl_name,) in tables_in_attached_db:
                        # Get schema information for each table
                        schema_info = con.execute(f"DESCRIBE {alias}.{tbl_name};").fetchall()
                        schema_description = (
                            f"CREATE TABLE {alias}.{tbl_name} (\n"
                            + ",\n".join([f"[{col[0]}] {col[1]}" for col in schema_info])
                            + "\n);"
                        )
                        # Store dataset info using alias and table name
                        datasets[f"{alias}_{tbl_name}"] = {"schema_description": schema_description}
                except Exception as e:
                    logging.error(f"Error attaching or reading schema from online DB {file_path}: {e}")
                    # Optionally, return an error response or skip this file
                    return JSONResponse(
                        content={"error": f"Error processing online database {file_path}: {e}"}, status_code=400
                    )
            else:
                # Handle local CSV file (existing logic)
                try:
                    df = pd.read_csv(file_path, encoding="utf-8", encoding_errors="replace", dayfirst=True)

                    dataset_name = os.path.splitext(os.path.basename(file_path))[0]
                    dataset_name = re.sub(r'[^a-zA-Z0-9_]', '_', dataset_name)
                    if dataset_name[0].isdigit():
                        dataset_name = f"t_{dataset_name}"

                    sanitized_columns = []
                    for col in df.columns:
                        sanitized_col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
                        if sanitized_col[0].isdigit():
                            sanitized_col = f"c_{sanitized_col}"
                        sanitized_columns.append(sanitized_col)
                    df.columns = sanitized_columns

                    con.execute(f"DROP TABLE IF EXISTS {dataset_name};")
                    con.register(f"data_table_{dataset_name}", df) # Use unique name for register
                    con.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM data_table_{dataset_name}")

                    schema_description = (
                        f"CREATE TABLE {dataset_name} (\n"
                        + ",\n".join(
                            [
                                f"[{col}] {dtype_mapping.get(str(df[col].dtype), 'TEXT')}"
                                for col in df.columns
                            ]
                        )
                        + "\n);"
                    )
                    datasets[dataset_name] = {"data": df, "schema_description": schema_description}
                except Exception as e:
                    logging.error(f"Error processing local file {file_path}: {e}")
                    return JSONResponse(
                        content={"error": f"Error processing local file {file_path}: {e}"}, status_code=400
                    )

        # Construct dataset_schemas string for LLM prompt
        dataset_schemas = ""
        if not datasets:
             return JSONResponse(
                content={"error": "No datasets could be loaded. Please check file paths or URLs."},
                status_code=400
            )

        for name, dataset_info in datasets.items():
            schema_description = dataset_info.get("schema_description")
            if schema_description and isinstance(schema_description, str):
                # For online DBs, name already includes alias. For local, it's the sanitized table name.
                dataset_schemas += f"Dataset name/identifier: {name}\nSchema: {schema_description}\n\n"

        if not dataset_schemas:
            return JSONResponse(
                content={"error": "Could not generate schema information for the provided datasets."},
                status_code=400
            )

        # User query
        user_query = request.query

        # Construct LLM prompt
        llm_prompt = (
            f"Here are the datasets available:\n{dataset_schemas}"
            f"Please write an duckDB query for the following question:\n{user_query}"
        )

        # Call LLM with the prompt
        llm_response = await call_llm_system_prompt(
            llm_prompt, 
            request.model, 
            request.api_base
        )

        # Extract the SQL query from the response
        sql_query_match = re.search(r"```sql\n(.*?)\n```", llm_response, re.DOTALL)
        if not sql_query_match:
            # Try alternative formats
            sql_query_match = re.search(r"```\n(.*?)\n```", llm_response, re.DOTALL)
            if not sql_query_match:
                return JSONResponse(content={
                    "error": "Failed to extract SQL query from the LLM response.",
                    "llm_response": llm_response,  # Include the full response for debugging
                    "prompt_used": llm_prompt  # Include the prompt that was used
                }, status_code=400)

        sql_query = sql_query_match.group(1).strip()
        
        # Fix common date syntax issues in DuckDB
        # Replace DATE() function with STRPTIME for multiple date formats
        date_func_pattern = r'DATE\s*\(\s*\[?([^)\]]+)\]?\s*\)'
        sql_query = re.sub(date_func_pattern, r"STRPTIME(\1, ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y', '%m-%d-%Y', '%d.%m.%Y', '%Y.%m.%d'])", sql_query)
        
        # Replace julianday() function with proper DuckDB date diff
        julianday_pattern = r'julianday\s*\(\s*\[?([^)\]]+)\]?\s*\)\s*-\s*julianday\s*\(\s*\[?([^)\]]+)\]?\s*\)'
        sql_query = re.sub(julianday_pattern, r"DATE_DIFF('day', STRPTIME(\2, ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y', '%m-%d-%Y', '%d.%m.%Y', '%Y.%m.%d']), STRPTIME(\1, ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d', '%d/%m/%Y', '%m-%d-%Y', '%d.%m.%Y', '%Y.%m.%d']))", sql_query)
        
        # Replace CAST to INTEGER with string comparison for invoice numbers
        invoice_cast_pattern = r'CAST\s*\(\s*([^)]+)\s*AS\s*INTEGER\s*\)'
        sql_query = re.sub(invoice_cast_pattern, r'\1', sql_query)
        
        # Log the extracted SQL query (for debugging)
        print(f"Extracted SQL Query: {sql_query}")

        # Execute the generated SQL query
        try:
            result = con.execute(sql_query).fetchdf()
        except Exception as query_error:
            # No need for special date handling anymore
            return JSONResponse(content={
                "error": f"Error executing query: {str(query_error)}",
                "generated_query": sql_query,
                "llm_response": llm_response
            }, status_code=400)

        # Convert any non-JSON serializable types to compatible formats
        result = result.apply(
            lambda col: col.map(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
        )
        
        # Handle non-JSON-compliant float values (NaN, inf) and Timestamp objects
        def sanitize_json_value(x):
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                return None
            if isinstance(x, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
                return x.isoformat()
            return x
            
        # Use map instead of applymap as per deprecation warning
        for col in result.columns:
            result[col] = result[col].map(sanitize_json_value)
        
        # Convert to dict and ensure all NaN values are handled
        result_dict = result.to_dict(orient="records")
        for row in result_dict:
            for key, value in row.items():
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    row[key] = None
        
        # Respond with the results
        if isinstance(llm_response, float):
            if (
                llm_response == float("inf")
                or llm_response == float("-inf")
                or (isinstance(llm_response, float) and llm_response != llm_response)
            ):
                llm_response = None  # or set to 0, depending on your needs
        return JSONResponse(
            content={
                "result": result_dict,
                "generated_query": sql_query,
                "llm_response": llm_response
            }
        )

    except Exception as e:
        return JSONResponse(content={
            "error": str(e),
            "llm_response": llm_response if 'llm_response' in locals() else None,
            "generated_query": sql_query if 'sql_query' in locals() else None,
            "prompt_used": llm_prompt if 'llm_prompt' in locals() else None
        }, status_code=400)


class SettingsRequest(BaseModel):
    key: str
    base: str


@app.post("/settings")
async def save_settings(request: SettingsRequest):
    # Save the settings to the environment variables
    os.environ["OPENAI_API_KEY"] = request.key
    os.environ["OPENAI_API_BASE"] = request.base
    with open(os.path.join(config_dir, "settings.json"), "w") as f:
        json.dump({"OPENAI_API_KEY": request.key, "OPENAI_API_BASE": request.base}, f)
    return {"status": "Settings saved successfully"}


# Mount static files directory LAST
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


def main():
    import uvicorn

    logger = logging.getLogger(__name__)
    PORT = int(os.getenv("PORT", 8001))

    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    if os.path.exists(os.path.join(config_dir, "settings.json")):
        with open(os.path.join(config_dir, "settings.json"), "r") as f:
            for key, value in json.load(f).items():
                os.environ[key] = value
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except BaseException as e:
        logger.error(f"Running locally. Cannot be accessed from outside: {e}")
        uvicorn.run(app, host="127.0.0.1", port=PORT)


if __name__ == "__main__":
    main()
