# Azure Functions Quickstart - Updated Generative AI Data Ingestion Functions 
#### Additional support for vector index curation

## Project Overview

The 'Azure Functions Quickstart - Generative AI Data Ingestion Functions' project is an Azure Durable Functions project aimed at streamlining the process of ingesting, chunking, and vectorizing PDF-based documents. These processes are critical for indexing and utilizing data in Retrieval Augmented Generation (RAG) patterns within Generative AI applications.

By leveraging Azure Durable Functions, the project orchestrates the complex workflows involved in data processing, ensuring efficiency and scalability. It includes capabilities for creating and managing Azure AI Search indexes, updating index aliases for deployment strategies, and indexing large volumes of pre-processed documents in bulk.

## Features
- **Ingestion and Chunking**: Automated breakdown of documents and audio files into chunks for easier processing.
- **Vectorization**: Transformation of textual and auditory information into vector embeddings suitable for AI models.
- **Index Management**: Tools for creating and updating Azure AI Search indexes to optimize data retrieval.
- **Workflow Orchestration**: Utilization of Durable Functions to coordinate and manage data processing tasks.
- **Postman Collection**: Sample postman collection (`AzFunc_IngestionOps.postman_collection`) demonstrating calling of all functions.

## Getting Started

### Prerequisites (Required Services)
- An active Azure subscription.
- Azure Function App.
- Azure Storage Account.
- Azure Cognitive Services, including Document Intelligence and Azure OpenAI.
- Azure AI Search Service instance.
- Azure Cosmos DB

### Installation
1. Clone the repository to your desired environment.
2. Install Azure Functions Core Tools if not already available.
3. In the project directory, install dependencies with `pip install -r requirements.txt`.

### Configuration
Configure the environment variables in your Azure Function App settings as follows:

| Variable Name                | Description                                               |
|------------------------------|-----------------------------------------------------------|
| `STORAGE_CONN_STR`           | Azure Storage account connection string                   |
| `DOC_INTEL_ENDPOINT`         | Endpoint for Azure Document Intelligence service          |
| `DOC_INTEL_KEY`              | Key for Azure Document Intelligence service               |
| `AOAI_KEY`                   | Key for Azure OpenAI service                              |
| `AOAI_ENDPOINT`              | Endpoint for Azure OpenAI service                         |
| `AOAI_EMBEDDINGS_MODEL`      | Model for generating embeddings with Azure OpenAI         |
| `AOAI_WHISPER_KEY`           | Key for Azure OpenAI Whisper model                        |
| `AOAI_WHISPER_ENDPOINT`      | Endpoint for Azure OpenAI Whisper model                   |
| `AOAI_WHISPER_MODEL`         | Model for transcribing audio with Azure OpenAI            |
| `SEARCH_ENDPOINT`            | Endpoint for Azure AI Search service                      |
| `SEARCH_KEY`                 | Key for Azure AI Search service                           |
| `SEARCH_SERVICE_NAME`        | Name of the Azure AI Search service instance              |
| `COSMOS_ENDPOINT`        | Endpoint for the Azure Cosmos DB instance              |
| `COSMOS_KEY`        | Key for the Azure Cosmos DB instance              |
| `COSMOS_DATABASE`        | Name of the Azure Cosmos DB database which will hold status records              |
| `COSMOS_CONTAINER`        | Name of the Azure Cosmos DB collection which will hold status records              |

<i>Note: review the `sample.settings.json` to create a `local.settings.json` environment file for local execution.</i>

### Deployment
The code contained within this repo can be deployed to your Azure Function app using the [deployment approaches outlined in this document](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies?tabs=windows). For initial deployment, we recommend using either the [Azure Functions Extension for VS Code](https://learn.microsoft.com/en-us/azure/azure-functions/functions-develop-vs-code?tabs=node-v4%2Cpython-v2%2Cisolated-process&pivots=programming-language-python) or the Azure Functions Core tools locally:

```
# Azure Functions Core Tools Deployment
func azure functionapp publish <YOUR-FUNCTION-APP-NAME> --publish-settings-only
```

## Utilization & Testing

Shown below are some of the common calls the created functions for creating, and populating an Azure AI Search index using files uploaded to Azure Blob Storage.

### Create Index (Manual Execution)

See `Create_Index` in Postman collection

POST to `http://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/create_new_index?code=<YOUR-FUNCTION-KEY>`
```
{
    "index_stem_name": "rag-index",
    "fields": {"content": "string", "pagenumber": "int", "sourcefile": "string", 
    "sourcepage": "string", "category": "string", "entra_id": "string", "session_id": "string"
    }
}
```

### Get Current Index 

See `Get_Active_Index` in Postman collection

GET to `http://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/get_active_index?code=<YOUR-FUNCTION-KEY>`
```
{
    "index_stem_name":"rag-index"
}
```


### Trigger PDF Ingestion

See `Trigger_PDF_Ingestion` in Postman collection

POST to `http://<YOUR-AZURE-FUNCTION-NAME>.azurewebsites.net/api/orchestrators/pdf_orchestration?code=<YOUR-FUNCTION-KEY>`
```
{
    "source_container": "<SOURCE_STORAGE_CONTAINER>",
    "extract_container": "<EXTRACT_STORAGE_CONTAINER>",
    "prefix_path": "<UPLOADED_FILE_PATH>",
    "entra_id": "<USER_ENTRA_ID>",
    "session_id": "<USER_SESSION_ID>",
    "index_name": "<YOUR_INDEX_NAME>",
    "cosmos_record_id": "<YOUR_COSMOS_LOG_RECORD_ID>"
    "automatically_delete": true
}
```

### 🧪 Preliminary Testing 🧪
To test your deployment and confirm everything is working as expected, use the [step-by-step testing guide](guides/deployment_testing.pdf) linked in this repo!

## Functions Deep Dive

### Orchestrators
The project contains orchestrators tailored for specific data types:
- `pdf_orchestrator`: Orchestrates the processing of PDF files, including chunking, extracting text & tables, generating embeddings, insertion into an Azure AI Search index, and cleanup of staged processing data.
- `index_documents_orchestrator`: Orchestrates the indexing of processed documents into Azure AI Search.
- `delete_documents_orchestrator`: Orchestrates the deletion of processed documents within your Azure Storage Account and Azure AI Search Index.

### Activities
The orchestrators utilize the following activities to perform discrete tasks:
- `get_source_files`: Retrieves a list of files from a specified Azure Storage container based on a user-provided prefix path.
- `delete_source_files`: Deletes files from a specified Azure Storage container based on a user-provided prefix path.
- `split_pdf_files`: Splits PDF files into individual pages and stores them as separate files.
- `process_pdf_with_document_intelligence`: Processes PDF chunks using Azure Document Intelligence and extracts relevant data.
- `generate_extract_embeddings`: Generates vector embeddings for the processed text data
- `insert_record`: Inserts processed data records into the Azure AI Search index.
- `check_containers`: Ensures that containers for intermediate data staging exist in Azure storage.


### Standalone Functions (HTTP Triggered)
In addition to orchestrators and activities, the project includes standalone functions for index management which can be triggered via a HTTP request:
- `create_new_index`: Creates a new Azure AI Search index with the specified fields.
- `get_active_index`: Retrieves the most current Azure AI Search index based on a user-provided root name.

### Standalone Functions (Scheduled Execution)
In addition to orchestrators and activities, the project includes standalone functions which execution on a timer. You can optionally update these functions to run on a different schedule (more or less frequently):
- `schedule_create_index`: Timer-executed function which creates a new index with a provided root name & predefined schema every X hours.
- `schedule_delete_index`: Timer-executed function which deletes indexes with a provided root name that are older than a user specified number of hours.


---
