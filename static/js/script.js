import { html, render } from "https://cdn.jsdelivr.net/npm/lit-html/lit-html.js";
import { unsafeHTML } from "https://cdn.jsdelivr.net/npm/lit-html@3/directives/unsafe-html.js";
import { Marked } from "https://cdn.jsdelivr.net/npm/marked@13/+esm";

const marked = new Marked();

const $openaiApiKey = document.getElementById("openai-api-key");
const $openaiApiBase = document.getElementById("openai-api-base");

// Define loading template
const loading = html` <div class="card">
  <div class="card-body text-center">
    <div class="spinner-border" role="status">
      <span class="d-none">Loading...</span>
    </div>
    <p class="mt-2">Loading...</p>
  </div>
</div>`;

// Consolidate common DOM element selections
const DOM = {
  output: () => document.getElementById("output"),
  responseOutput: () => document.getElementById("responseOutput"),
  queryInput: () => document.getElementById("queryInput"),
  filePathInput: () => document.getElementById("filePathInput"),
  executeButton: () => document.getElementById("executeButton"),
};

document.addEventListener("DOMContentLoaded", () => {
  const loadFileButton = document.getElementById("loadFileButton");
  const executeButton = document.getElementById("executeButton");
  loadFileButton.addEventListener("click", loadFile);
  executeButton.addEventListener("click", executeQuery);
  // Initialize the output area
  const output = document.getElementById("output");
  if (output) {
    render(html``, output);
  }
});

function renderOutput(data) {
  const output = document.getElementById("output");
  if (!output) {
    console.error("Output element not found");
    return;
  }

  // Render output for all datasets
  const template = html`
    <div>
      ${data.uploaded_datasets.map(
        (dataset, index) =>
          html`
            <div class="card mb-3">
              <div class="card-header">
                <h5>
                  Dataset ${index + 1}: ${dataset.dataset_name}
                  <span class="badge bg-secondary">${dataset.file_type}</span>
                </h5>
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
                    .map((question) => html`<li class="list-group-item">${question}</li>`)}
                </ul>
              </div>
            </div>
          `
      )}
    </div>
  `;
  render(template, output);
}

function parseSchema(schemaString) {
  // Match the table creation syntax with column definitions
  const match = schemaString.match(/\(([\s\S]*?)\)/); // Match everything inside parentheses
  if (!match) {
    renderError("Invalid schema format. Unable to extract column definitions.");
    return [];
  }
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

// Simplified error handling
function renderError(errorMessage) {
  const errorTemplate = html`
    <div class="alert alert-danger" role="alert"><strong>Error:</strong> ${errorMessage}</div>
  `;
  render(errorTemplate, DOM.output() || DOM.responseOutput());
}

// Optimized executeQuery function
async function executeQuery() {
  const responseOutput = DOM.responseOutput();
  if (!responseOutput) return;

  render(loading, responseOutput);
  const query = DOM.queryInput()?.value.trim();
  const filePath = DOM.filePathInput()?.value.trim();

  if (!query) {
    renderError("Please enter a valid query.");
    return;
  }

  try {
    const response = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dataset_name: "dataset", query, file_path: filePath }),
    });

    if (!response.ok) throw new Error(`Error executing query: ${response.statusText}`);

    const result = await response.json();
    const queryOutput = html`
      <div class="card">
        <div class="card-header">
          <h5>Query Result</h5>
        </div>
        <div class="card-body">
          <h6>Response from LLM:</h6>
          <div>${unsafeHTML(marked.parse(result.llm_response))}</div>
          <h6>SQL Query Execution Result:</h6>
          <div id="sqlResultTable"></div>
          <button class="btn btn-primary mt-3" @click=${() => downloadCSV(result.result, "query_result.csv")}>
            <i class="bi bi-download"></i> Download Results as CSV
          </button>
        </div>
      </div>
    `;

    render(queryOutput, responseOutput);
    document.getElementById("sqlResultTable").innerHTML = generateTable(result.result);
  } catch (error) {
    renderError(error.message);
  }
}

// Optimized loadFile function
async function loadFile() {
  const output = DOM.output();
  const filePath = DOM.filePathInput()?.value.trim();

  if (!output || !filePath) {
    renderError("Please enter a valid file path.");
    return;
  }

  render(loading, output);
  try {
    const response = await fetch("/upload", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ file_path: filePath }),
    });

    if (!response.ok) throw new Error(`Error loading file: ${response.statusText}`);

    const data = await response.json();
    renderOutput(data);
    DOM.executeButton()?.removeAttribute("disabled");
  } catch (error) {
    console.error(error);
    renderError(error.message);
  }
}

// Optimized table generation
function generateTable(data) {
  if (!Array.isArray(data) || !data.length) return "";

  const headers = Object.keys(data[0]);
  return `
        <table class="table table-bordered table-striped">
            <thead>
                <tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr>
            </thead>
            <tbody>
                ${data
                  .map((row) => `<tr>${headers.map((header) => `<td>${row[header] ?? ""}</td>`).join("")}</tr>`)
                  .join("")}
            </tbody>
        </table>
    `;
}

// Optimized CSV conversion and download
function convertToCSV(data) {
  if (!Array.isArray(data) || !data.length) return "";

  const headers = Object.keys(data[0]);
  return [
    headers.join(","),
    ...data.map((row) => headers.map((header) => JSON.stringify(row[header] ?? "")).join(",")),
  ].join("\n");
}

function downloadCSV(data, filename = "data.csv") {
  const csv = convertToCSV(data);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });

  if (navigator.msSaveBlob) {
    navigator.msSaveBlob(blob, filename);
    return;
  }

  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  link.click();
  URL.revokeObjectURL(link.href);
}

document.getElementById("settings").addEventListener("submit", async (event) => {
  event.preventDefault();
  document.querySelector("#settings .loading").classList.remove("d-none");
  await fetch("/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key: $openaiApiKey.value, base: $openaiApiBase.value }),
  });
  localStorage.setItem("localDataChatOpenAIAPIKey", $openaiApiKey.value);
  localStorage.setItem("localDataChatOpenAIAPIBase", $openaiApiBase.value);
  document.querySelector("#settings .loading").classList.add("d-none");
  document.querySelector("#settings .saved").classList.remove("d-none");
  setTimeout(() => {
    document.querySelector("#settings .saved").classList.add("d-none");
    document.querySelector("#settings").classList.remove("show");
  }, 2000);
});

document.querySelector("#openai-api-key").value = localStorage.getItem("localDataChatOpenAIAPIKey");
document.querySelector("#openai-api-base").value = localStorage.getItem("localDataChatOpenAIAPIBase") ?? "https://llmfoundry.straive.com/openai/v1";
if (!document.querySelector("#openai-api-key").value)
  document.querySelector("#settings").classList.add("show");
