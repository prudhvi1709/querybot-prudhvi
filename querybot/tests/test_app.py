import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import pandas as pd
import duckdb # Required for type hints and potentially for creating mock objects

# Import the FastAPI app instance
# Assuming your FastAPI app instance is named 'app' in 'querybot.app'
from querybot.app import app, SYSTEM_PROMPT # Import SYSTEM_PROMPT if needed for some tests

@pytest.fixture(scope="module")
def client():
    """
    Test client for the FastAPI application.
    """
    with TestClient(app) as c:
        yield c

@pytest.fixture(autouse=True)
def clear_datasets_fixture():
    """
    Fixture to automatically clear the 'datasets' dictionary in app.py
    before each test that uses it. This prevents state leakage between tests.
    """
    from querybot.app import datasets # import here to get the actual dict
    datasets.clear()
    yield # Test runs here
    datasets.clear()


# Placeholder for a valid DuckDB URL for testing
TEST_DUCKDB_URL = "http://example.com/test.db"

# Basic test to ensure the client fixture works
def test_read_main(client):
    response = client.get("/") # Assuming you have a root path, adjust if not
    # This will likely be a 404 if you serve static files on / and don't have index.html in tests
    # Or, if you have a health check endpoint, use that.
    # For now, let's assume a 404 is acceptable for a non-existent root in test if static files aren't set up for test client.
    # Or, if your app has a specific endpoint like /status or /health, use that.
    # If your app serves static files and html=True on root, without an index.html it might be 404.
    # This test is mostly to ensure the client can be initialized.
    # A more robust check would be against a known simple endpoint if one exists.
    assert response.status_code in [200, 404] # Adjust as per your app's root behavior

# More tests will be added here.

# Example of how to mock pandas read_csv if needed in other tests
# @patch('pandas.read_csv')
# def test_example_with_mocked_read_csv(mock_read_csv, client):
#     mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
#     mock_read_csv.return_value = mock_df
#     # ... rest of your test
#     pass

# Example of how to mock app.con (global duckdb connection)
# @patch('querybot.app.con')
# def test_example_with_mocked_global_con(mock_duckdb_con, client):
#     # Setup mock_duckdb_con behavior
#     # mock_duckdb_con.execute().fetchall()... or .fetchdf()
#     pass

# Example of how to mock call_llm_system_prompt
# @patch('querybot.app.call_llm_system_prompt')
# async def test_example_with_mocked_llm(mock_llm_call, client):
#     mock_llm_call.return_value = "SELECT * FROM mock_table;"
#     # ... rest of your test
#     pass

# We will add specific tests for get_schema_from_duckdb and /query endpoint next.
# Ensure that the global 'con' from app.py is used or mocked appropriately in tests.
# Global 'con' is initialized when app.py is imported.
# For tests involving database operations, it might need to be reset or carefully managed if tests modify its state.
# However, for many /query tests, we'll be mocking its 'execute' method anyway.

# If your app connects to external services on startup (outside of endpoint calls),
# those might need mocking at the module level or via fixtures.
# The current app.py initializes a DuckDB in-memory DB and loads extensions, which is fine.
# It doesn't seem to make external calls on import.
# OPENAI_API_KEY and OPENAI_API_BASE are loaded via load_dotenv(),
# so ensure you have a .env file or set these env vars if tests call actual LLM.
# For these tests, we will mock LLM calls.

# Import the function to be tested
from querybot.app import get_schema_from_duckdb

def test_get_schema_from_duckdb_with_url():
    mock_url = TEST_DUCKDB_URL # "http://example.com/test.db"

    # Mock the duckdb.connect call
    with patch('duckdb.connect') as mock_connect:
        # Configure the mock connection object
        mock_con_instance = MagicMock()
        mock_connect.return_value = mock_con_instance

        # Mock chain for con.execute(...).fetchall()
        # 1. SHOW TABLES;
        mock_execute_show_tables = MagicMock()
        mock_execute_show_tables.fetchall.return_value = [('test_remote_table',)]

        # 2. DESCRIBE test_remote_table;
        mock_execute_describe = MagicMock()
        mock_execute_describe.fetchall.return_value = [
            ('col_a', 'INTEGER', None, None, None, None),
            ('col_b', 'VARCHAR', None, None, None, None)
        ] # DuckDB describe returns more columns, only first two are used by the function

        # 3. SELECT * FROM test_remote_table LIMIT 5;
        mock_execute_select_limit = MagicMock()
        sample_data_rows = [(1, 'value1'), (2, 'value2')]
        mock_execute_select_limit.fetchall.return_value = sample_data_rows

        # Configure side_effect for multiple calls to con.execute
        mock_con_instance.execute.side_effect = [
            mock_execute_show_tables,
            mock_execute_describe,
            mock_execute_select_limit
        ]

        # Call the function
        schema_desc, sample_data = get_schema_from_duckdb(mock_url)

        # Assertions
        mock_connect.assert_called_once_with(mock_url, read_only=True)

        assert mock_con_instance.execute.call_count == 3
        mock_con_instance.execute.assert_any_call("SHOW TABLES;")
        mock_con_instance.execute.assert_any_call("DESCRIBE test_remote_table;")
        mock_con_instance.execute.assert_any_call("SELECT * FROM test_remote_table LIMIT 5;")

        expected_schema_desc = "CREATE TABLE test_remote_table (\n[col_a] INTEGER,\n[col_b] VARCHAR\n);"
        assert schema_desc == expected_schema_desc
        assert sample_data == sample_data_rows

        mock_con_instance.close.assert_called_once()

def test_get_schema_from_duckdb_with_url_connection_error():
    mock_url = "http://example.com/nonexistent.db"
    with patch('duckdb.connect', side_effect=duckdb.IOException("Connection failed")) as mock_connect:
        with pytest.raises(ValueError) as excinfo:
            get_schema_from_duckdb(mock_url)
        assert f"Could not connect to DuckDB URL: {mock_url}" in str(excinfo.value)
        mock_connect.assert_called_once_with(mock_url, read_only=True)

def test_get_schema_from_duckdb_with_url_no_tables():
    mock_url = TEST_DUCKDB_URL
    with patch('duckdb.connect') as mock_connect:
        mock_con_instance = MagicMock()
        mock_connect.return_value = mock_con_instance

        mock_execute_show_tables = MagicMock()
        mock_execute_show_tables.fetchall.return_value = [] # No tables
        mock_con_instance.execute.return_value = mock_execute_show_tables

        with pytest.raises(ValueError) as excinfo:
            get_schema_from_duckdb(mock_url)
        assert "No tables found in the online DuckDB database" in str(excinfo.value)
        mock_con_instance.close.assert_called_once()

# Tests for /query endpoint with online URL
@patch('querybot.app.call_llm_system_prompt')
@patch('querybot.app.con') # Mock the global connection object from app.py
def test_query_endpoint_with_online_url(mock_app_con, mock_llm_call, client):
    mock_url = TEST_DUCKDB_URL

    # --- Setup mock for app.con (global connection) ---
    # This mock will handle ATTACH, information_schema query, DESCRIBE, and the LLM query

    # 1. For ATTACH: Successful execution
    # (ATTACH command doesn't have a direct result to fetchall/fetchdf)

    # 2. For information_schema.tables query
    mock_info_schema_execute = MagicMock()
    mock_info_schema_execute.fetchall.return_value = [('remote_table1',)]

    # 3. For DESCRIBE remote_table1
    mock_describe_execute = MagicMock()
    mock_describe_execute.fetchall.return_value = [
        ('id', 'INTEGER', None, None, None, None),
        ('data', 'VARCHAR', None, None, None, None)
    ]

    # 4. For the LLM-generated query
    mock_llm_query_execute = MagicMock()
    expected_query_result_df = pd.DataFrame({'id': [1, 2], 'data': ['test', 'live']})
    mock_llm_query_execute.fetchdf.return_value = expected_query_result_df

    # Configure side_effect for multiple app.con.execute calls
    # Order: ATTACH, information_schema, DESCRIBE, LLM_QUERY
    mock_app_con.execute.side_effect = [
        MagicMock(), # For ATTACH - no specific fetch method needed after it
        mock_info_schema_execute,
        mock_describe_execute,
        mock_llm_query_execute
    ]

    # --- Setup mock for LLM call ---
    # The LLM should receive a prompt containing the schema of online_db_0.remote_table1
    # and should return a query that uses this table.
    expected_llm_sql_query = "SELECT id, data FROM online_db_0.remote_table1 WHERE id = 1;"
    mock_llm_call.return_value = f"```sql\n{expected_llm_sql_query}\n```"

    # --- Make the request ---
    response = client.post(
        "/query",
        json={
            "dataset_name": "ignored_for_this_test_as_path_is_url", # Or make it relevant
            "query": "Get data for id 1 from remote table",
            "file_path": mock_url, # This is the online URL
            "is_explanation": False
        }
    )

    # --- Assertions ---
    assert response.status_code == 200
    response_json = response.json()

    assert response_json["generated_query"] == expected_llm_sql_query
    assert response_json["result"] == expected_query_result_df.to_dict(orient="records")

    # Verify app.con.execute calls
    assert mock_app_con.execute.call_count == 4
    mock_app_con.execute.assert_any_call(f"ATTACH '{mock_url}' AS online_db_0 (READ_ONLY);")
    mock_app_con.execute.assert_any_call("SELECT table_name FROM information_schema.tables WHERE table_schema = 'online_db_0'")
    mock_app_con.execute.assert_any_call("DESCRIBE online_db_0.remote_table1;")
    mock_app_con.execute.assert_any_call(expected_llm_sql_query) # The query from LLM

    # Verify LLM call
    mock_llm_call.assert_called_once()
    llm_prompt_args = mock_llm_call.call_args[0]
    llm_prompt_input = llm_prompt_args[0] # The 'user_input' argument to call_llm_system_prompt

    assert "Dataset name/identifier: online_db_0_remote_table1" in llm_prompt_input
    assert "CREATE TABLE online_db_0.remote_table1 (\n[id] INTEGER,\n[data] VARCHAR\n);" in llm_prompt_input
    assert "Get data for id 1 from remote table" in llm_prompt_input


@patch('querybot.app.call_llm_system_prompt')
@patch('querybot.app.con')
def test_query_endpoint_with_online_url_attach_fails(mock_app_con, mock_llm_call, client):
    mock_url = "http://example.com/fail_attach.db"

    # Simulate ATTACH failure
    mock_app_con.execute.side_effect = duckdb.IOException("Failed to attach database")

    response = client.post(
        "/query",
        json={
            "dataset_name": "test_fail",
            "query": "Any query",
            "file_path": mock_url,
            "is_explanation": False
        }
    )

    assert response.status_code == 400
    response_json = response.json()
    assert "Error processing online database" in response_json["error"]
    assert "Failed to attach database" in response_json["error"]

    mock_app_con.execute.assert_called_once_with(f"ATTACH '{mock_url}' AS online_db_0 (READ_ONLY);")
    mock_llm_call.assert_not_called() # LLM should not be called if attach fails
