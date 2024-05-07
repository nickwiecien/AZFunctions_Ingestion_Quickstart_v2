from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents import SearchClient
from azure.search.documents.indexes.models import (
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchField,
    SearchField,  
    VectorSearch,  
    HnswAlgorithmConfiguration, 
    VectorSearchProfile
)
import os
from datetime import datetime
import requests
import json
# from azure.search.documents.indexes.models import HnswAlgorithmConfiguration

def get_current_index(index_stem_name):
    """
    Retrieves existing Azure AI search indexes (based on a provided prefix) and returns the 
    most recently created index to the user by name.

    Args:
    index_stem_name (str): The stem of the index name to filter out relevant indexes.
    """
    # Get the search key, endpoint, and service name from environment variables
    search_key = os.environ['SEARCH_KEY']
    search_endpoint = os.environ['SEARCH_ENDPOINT']
    search_service_name = os.environ['SEARCH_SERVICE_NAME']
    
    # Connect to Azure Cognitive Search resource using the provided key and endpoint
    credential = AzureKeyCredential(search_key)
    client = SearchIndexClient(endpoint=search_endpoint, credential=credential)
    
    # List all indexes in the search service
    indexes = client.list_index_names()
    
    # Find all indexes starting with the given stem name
    matching_indexes = [i for i in indexes if i.startswith(index_stem_name)]
    print(matching_indexes)

    # Parse the timestamp from each index name and store in a dictionary
    timestamp_to_index_dict = {}
    for index_name in matching_indexes:
        parts = index_name.split('-')
        timestamp = parts[-1]
        parsed_timestamp = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        timestamp_to_index_dict[parsed_timestamp] = index_name

    # Sort timestamps from oldest to newest
    timestamps = sorted(timestamp_to_index_dict.keys())
    
    # Get the newest index based on timestamps
    newest_index = timestamp_to_index_dict[timestamps[-1]]

    # Get the index details
    index = client.get_index(newest_index)
    fields = [f.name for f in index.fields]

    return newest_index, fields


def delete_indexes(index_stem_name, age_in_minutes=60):
    """
    Retrieves indexes (by matching stem name) and deletes those which are older than
    the user-provided age_in_minutes argument. Used for index cleanup operations.

    Args:
    index_stem_name (str): The stem of the index name to filter out relevant indexes.
    age_in_minutes (int): The age (in minutes) at which indexes should be deleted.
    """
    # Get the search key, endpoint, and service name from environment variables
    search_key = os.environ['SEARCH_KEY']
    search_endpoint = os.environ['SEARCH_ENDPOINT']
    search_service_name = os.environ['SEARCH_SERVICE_NAME']
    
    # Connect to Azure Cognitive Search resource using the provided key and endpoint
    credential = AzureKeyCredential(search_key)
    client = SearchIndexClient(endpoint=search_endpoint, credential=credential)
    
    # List all indexes in the search service
    indexes = client.list_index_names()

    # Find all indexes starting with the given stem name
    matching_indexes = [i for i in indexes if i.startswith(index_stem_name)]
    print(matching_indexes)

    # Parse the timestamp from each index name and store in a dictionary
    indexes_to_delete = []
    for index_name in matching_indexes:
        parts = index_name.split('-')
        timestamp = parts[-1]
        parsed_timestamp = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        timedelta = datetime.now() - parsed_timestamp
        if timedelta.total_seconds() > age_in_minutes * 60:
            indexes_to_delete.append(index_name)

    # Delete the indexes that are older than the specified age
    for index_name in indexes_to_delete:
        client.delete_index(index_name)

    return indexes_to_delete
   

def insert_documents_vector(documents, index_name):
    """
    Inserts a document vector into the specified search index on Azure Cognitive Search.

    Args:
    documents (list): The list of documents to insert.
    index_name (str): The name of the search index.
    """
    # Get the search key, endpoint, and service name from environment variables
    search_key = os.environ['SEARCH_KEY']
    search_endpoint = os.environ['SEARCH_ENDPOINT']
    search_service_name = os.environ['SEARCH_SERVICE_NAME']

    # Create a SearchClient object
    credential = AzureKeyCredential(search_key)
    client = SearchClient(endpoint=search_endpoint, index_name=index_name, credential=credential)

    # Upload the document to the search index
    result = client.upload_documents(documents=documents)

    return result

def create_vector_index(stem_name, user_fields):
    # Get the search key, endpoint, and service name from environment variables
    search_key = os.environ['SEARCH_KEY']
    search_endpoint = os.environ['SEARCH_ENDPOINT']
    search_service_name = os.environ['SEARCH_SERVICE_NAME']

    # Get the current time and format it as a string
    now =  datetime.now()
    timestamp  = datetime.strftime(now, "%Y%m%d%H%M%S")

    # Create the index name by appending the timestamp to the stem name
    index_name  = f'{stem_name}-{timestamp}'

    # Create a SearchIndexClient object
    credential = AzureKeyCredential(search_key)
    client = SearchIndexClient(endpoint=search_endpoint, credential=credential)

    # Define the fields for the index
    fields = [SimpleField(name="id", type=SearchFieldDataType.String, key=True)]
    
    # Add user-defined fields to the index
    for field, field_type in user_fields.items():
        if field_type == 'string':
            fields.append(SearchableField(name=field, type=SearchFieldDataType.String, searchable=True,  filterable=True))
        elif field_type == 'int':
            fields.append(SimpleField(name=field, type=SearchFieldDataType.Int32, searchable=False, filterable=True))
        elif field_type == 'datetime':
            fields.append(SimpleField(name=field, type=SearchFieldDataType.DateTimeOffset, searchable=False, filterable=True))
        elif field_type == 'double':
            fields.append(SimpleField(name=field, type=SearchFieldDataType.Double, searchable=False, filterable=True))
        elif field_type == 'bool':
            fields.append(SimpleField(name=field, type=SearchFieldDataType.Boolean, searchable=False, filterable=True))

    # Add a field for vector embeddings
    fields = fields + [ SearchField(name="embeddings", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    searchable=True, vector_search_dimensions=1536, vector_search_profile_name="vector-config")]
    
    # Define vector search configurations
    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="algorithm-config",
            )
        ],
        profiles=[VectorSearchProfile(name="vector-config", algorithm_configuration_name="algorithm-config")],
    )

    # Create the search index with the specified fields and vector search configuration
    index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
    result = client.create_or_update_index(index)

    return result.name


def create_update_index_alias(alias_name, target_index):
    """
    Creates or updates an alias for a given Azure Cognitive Search index.

    Args:
    search_service_name (str): The name of the Azure Cognitive Search service.
    search_key (str): The admin key of the Azure Cognitive Search service.
    alias_name (str): The name of the alias to create or update.
    target_index (str): The name of the index that the alias should point to.
    """

    # Get the search key, endpoint, and service name from environment variables
    search_key = os.environ['SEARCH_KEY']
    search_endpoint = os.environ['SEARCH_ENDPOINT']
    search_service_name = os.environ['SEARCH_SERVICE_NAME']

    # Construct the URI for alias creation
    uri = f'https://{search_service_name}.search.windows.net/aliases?api-version=2023-07-01-Preview'
    headers = {'Content-Type': 'application/json', 'api-key': search_key}
    payload = {
        "name": alias_name,
        "indexes": [target_index]
    }
    
    try:
        # Attempt to create the alias
        response = requests.post(uri, headers=headers, data=json.dumps(payload))
        # If alias creation fails with a 400 error, update the existing alias
        if response.status_code == 400:
            uri = f'https://{search_service_name}.search.windows.net/aliases/{alias_name}?api-version=2023-07-01-Preview'
            response = requests.put(uri, headers=headers, data=json.dumps(payload))
    except Exception as e:
        print(e)