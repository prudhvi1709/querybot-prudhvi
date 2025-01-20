import { html, render } from "https://cdn.jsdelivr.net/npm/lit-html/lit-html.js";
// import { Marked } from "https://cdn.jsdelivr.net/npm/marked@13/+esm";

let uploadedDatasetName;
let tableName;
const loading = html`<div class="spinner-border" role="status">
  <span class="visually-hidden">Loading...</span>
</div>`;   
document.addEventListener("DOMContentLoaded", () => {
    const executeButton = document.getElementById("executeButton");
    const downloadButton = document.getElementById("downloadButton");

    executeButton.addEventListener("click", executeQuery);
    downloadButton.addEventListener("click", downloadTable);
});
// File input change handler for multiple file upload
document.getElementById("fileInput").addEventListener("change", function () {
    const output = document.getElementById("output");
    const files = document.getElementById("fileInput").files;
    render(loading,output);
    if (!files || files.length === 0) {
        console.error("No files selected.");
        return;
    }
    const formData = new FormData();
    for (let i = 0; i < files.length; i++) {
        formData.append("files", files[i]);
    }

    // Send the POST request to the backend to upload files
    fetch("http://127.0.0.1:8020/upload_csv/", {
        method: "POST",
        body: formData,
    })
        .then((response) => {
            if (!response.ok) {
                throw new Error(`Error uploading CSV: ${response.statusText}`);
            }
            return response.json();
        })
        .then((data) => {
            console.log("Upload Response:", data);
            renderOutput(data);
            uploadedDatasetName = data.uploaded_datasets[0].dataset_name;
            tableName = uploadedDatasetName;
            console.log("Uploaded dataset name:", uploadedDatasetName);
            document.getElementById("executeButton").disabled = false;
        })
        .catch((error) => {
            console.error(error);
            renderError(error.message);
            // document.getElementById("output").innerText = `Error: ${error.message}`;
            document.getElementById("executeButton").disabled = true;
        });
});
function renderOutput(data) {
    // Render output for all datasets
    const output = html`
    <div>
      ${data.uploaded_datasets.map(
        (dataset, index) =>
            html`
            <div class="card mb-3">
              <div class="card-header">
                <h5>Dataset ${index + 1}: ${dataset.dataset_name}</h5>
              </div>
              <div class="card-body">
                <h6 class="card-title">Schema:</h6>
                <table class="table table-bordered">
                  <thead>
                    <tr>
                      <th>Column Name</th>
                      <th>Data Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${parseSchema(dataset.schema).map(
                (col) =>
                    html`
                          <tr>
                            <td>${col.name}</td>
                            <td>${col.type}</td>
                          </tr>
                        `
            )}
                  </tbody>
                </table>
                <h6 class="card-title">Suggested Questions:</h6>
                <ul class="list-group">
                  ${dataset.suggested_questions
                    .split("\n")
                    .map(
                        (question) =>
                            html`<li class="list-group-item">${question}</li>`
                    )}
                </ul>
              </div>
            </div>
          `
    )}
    </div>
  `;
    render(output, document.getElementById("output"));
}

function parseSchema(schemaString) {
    // Match the table creation syntax with column definitions
    const match = schemaString.match(/\(([\s\S]*?)\)/); // Match everything inside parentheses
    if (!match) {
        renderError("Invalid schema format. Unable to extract column definitions.");
        return [];
    }

    // Extract and clean up column definitions
    const columnDefinitions = match[1]
        .split(",")
        .map((col) => col.trim())
        .filter(Boolean); // Remove empty strings

    // Parse each column definition into name and type
    return columnDefinitions.map((colDef) => {
        const parts = colDef.match(/\[([^\]]+)\] (\w+)/); // Match [column_name] data_type
        if (!parts) {
            return { name: "Unknown", type: "Unknown" };
        }
        return {
            name: parts[1], // Extract column name
            type: parts[2], // Extract data type
        };
    });
}

function renderError(errorMessage) {
    const errorOutput = html`
    <div class="alert alert-danger" role="alert">
      <strong>Error:</strong> ${errorMessage}
    </div>
  `;
    render(errorOutput, document.getElementById("output"));
}

// Function to execute the query
async function executeQuery() {
    const responseOutput = document.getElementById("responseOutput");
    render(loading, responseOutput);
    const query = document.getElementById("queryInput").value.trim();
    console.log("Executing query:", query);

    if (!uploadedDatasetName) {
        console.error("No dataset uploaded. Please upload a CSV file first.");
        renderError("No dataset uploaded. Please upload a CSV file first.");
        return;
    }

    if (!query) {
        console.error("Please enter a valid query.");
        renderError("Please enter a valid query.");
        return;
    }

    try {
        const response = await fetch("http://127.0.0.1:8020/query/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                dataset_name: uploadedDatasetName,
                query: query,
            }),
        });

        if (!response.ok) {
            throw new Error(`Error executing query: ${response.statusText}`);
        }

        const result = await response.json();
        const sqlQuery = extractSQLQuery(result.llm_response);
        const queryOutput = html`
      <div class="card">
        <div class="card-header">
          <h5>Query Result</h5>
        </div>
        <div class="card-body">
          <h6>Response from LLM:</h6>
          <pre class="rounded border p-2">${result.llm_response}</pre>
          <h6>SQL Query Execution Result:</h6>
          <div id="sqlResultTable"></div>
        </div>
      </div>
    `;
        // Function to generate table from JSON data
        function generateTable(data) {
            if (!data || typeof data !== "object") return "";
            const headers = Object.keys(data[0]);
            let tableHTML =
                '<table class="table table-bordered table-striped"><thead><tr>';
            headers.forEach((header) => {
                tableHTML += `<th>${header}</th>`;
            });
            tableHTML += "</tr></thead><tbody>";

            // Iterate through each row of data and create table rows
            data.forEach((row) => {
                tableHTML += "<tr>";
                headers.forEach((header) => {
                    tableHTML += `<td>${row[header]}</td>`;
                });
                tableHTML += "</tr>";
            });
            tableHTML += "</tbody></table>";

            return tableHTML;
        }
        function convertToCSV(data) {
            const headers = Object.keys(data[0]);
            const rows = data.map((row) =>
                headers.map((header) => row[header]).join(",")
            );
            return [headers.join(","), ...rows].join("\n");
        }
        function downloadCSV(data) {
            const csvData = convertToCSV(data);
            const blob = new Blob([csvData], { type: "text/csv;charset=utf-8;" });
            const link = document.createElement("a");
            link.href = URL.createObjectURL(blob);
            link.download = "query_result.csv";
            link.click();
        }
        // Render the query output
        render(queryOutput, document.getElementById("responseOutput"));
        document.getElementById("sqlResultTable").innerHTML = generateTable(
            result.result
        );
        // Download table as CSV
        const downloadButton = document.createElement("button");
        downloadButton.id = "downloadCSV";
        downloadButton.classList.add("btn", "btn-primary", "mt-3");
        downloadButton.textContent = "Download CSV";
        const sqlResultTable = document.getElementById("sqlResultTable");
        sqlResultTable.parentNode.insertBefore(
            downloadButton,
            sqlResultTable.nextSibling
        );
        downloadButton.addEventListener("click", () => {
            downloadCSV(result.result);
        });
    } catch (error) {
        console.error(error);
        renderError(error.message);
    }
}
function extractSQLQuery(llmResponse) {
    const match = llmResponse.match(/```sql\n([\s\S]*?)\n```/);
    return match ? match[1].trim() : null;
}
document.getElementById("executeButton").disabled = true;
function downloadTable() {
    if (!tableName) {
        console.error("No table name available for download.");
        return;
    }
    window.location.href = `http://127.0.0.1:8020/download/${tableName}`;
}
document.getElementById("downloadButton").addEventListener("click", downloadTable);
