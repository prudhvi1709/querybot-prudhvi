# Local Datachat

A web application that allows users to upload CSV files, execute queries, and download results.

## Description

This project is built using FastAPI for the backend and HTML with Bootstrap for the frontend. Users can upload datasets, execute SQL queries, and interact with the data through a user-friendly interface.

## Installation Instructions

1. Clone the repository:
   ```bash
   git clone https://github.com/prudhvi1709/localdatachat.git
   ```
2. Navigate to the project directory:
   ```bash
   cd localdatachat
   ```
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set the environment variable:
   To set the environment variable LLMFOUNDRY_TOKEN, follow these steps:

   **For Linux/Mac:**
   ```bash
   export LLMFOUNDRY_TOKEN="your_token_here"
   ```
   **For Windows:**
   ```bash
   set LLMFOUNDRY_TOKEN="your_token_here"
   ```
   Replace `"your_token_here"` with your actual LLMFOUNDRY token.

## Usage

1. Start the FastAPI server:
   ```bash
   uvicorn app:app --reload --port 8020
   ```
2. In a new terminal window, run the following command to start the frontend:
   ```bash
   python -m http.server 8030
   ```
   Then, open your web browser and navigate to (http://localhost:8030) to access the Local Datachat application.
3. Use the interface to upload CSV files, execute queries, and download results.

## File Structure

```
/project-directory
│
├── app.py              # The main Python application file
├── index.html          # The main HTML file for the frontend
├── requirements.txt    # The list of required Python packages
├── LICENSE             # The project license file
└── README.md           # The project README file
```

## Features
- **Upload CSV Files**: Users can upload datasets through the web interface.
- **Execute Queries**: Users can execute SQL queries against uploaded datasets.
- **Download Results**: Users can download query results in a convenient format.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
