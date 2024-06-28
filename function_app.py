import azure.functions as func
import azure.durable_functions as df
import logging
import json
import os
import hashlib
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from pypdf import PdfReader, PdfWriter
from io import BytesIO
import tempfile
import re
import requests
from datetime import datetime
import filetype
import time

from doc_intelligence_utilities import analyze_pdf, extract_results
from aoai_utilities import generate_embeddings, get_transcription
from ai_search_utilities import create_vector_index, get_current_index, create_update_index_alias, insert_documents_vector, delete_indexes

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    payload = json.loads(req.get_body())

    instance_id = await client.start_new(function_name, client_input=payload)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrators
@app.orchestration_trigger(context_name="context")
def pdf_orchestrator(context):

    first_retry_interval_in_milliseconds = 5000
    max_number_of_attempts = 2
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    ###################### DATA INGESTION START ######################
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container names from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    prefix_path = payload.get("prefix_path")
    entra_id = payload.get("entra_id")
    session_id = payload.get("session_id")
    index_name = payload.get("index_name")
    index_stem_name = payload.get("index_stem_name")
    cosmos_record_id = payload.get("cosmos_record_id")
    automatically_delete = payload.get("automatically_delete")

    # Get status record from Cosmos Database - continue on if no record is found
    try:
        payload = yield context.call_activity("get_status_record", json.dumps({'cosmos_id': cosmos_record_id, 'entra_id': entra_id}))
        context.set_custom_status('Retrieved Cosmos Record Successfully')
    except Exception as e:
        context.set_custom_status('Cosmos Record Not Found')
        pass

    # Create a status record that can be used to update CosmosDB
    try:
        status_record = payload
        status_record['id'] = cosmos_record_id
        status_record['status'] = 1
        status_record['status_message'] = 'Starting Ingestion Process'
        status_record['processing_progress'] = 0.1
        yield context.call_activity("update_status_record", json.dumps(status_record))
    except Exception as e:
        pass

    # Define intermediate containers that will hold transient data
    chunks_container = f'{source_container}-chunks'
    doc_intel_results_container = f'{source_container}-doc-intel-results'

    # Confirm that all storage locations exist to support document ingestion
    try:
        container_check = yield context.call_activity_with_retry("check_containers", retry_options, json.dumps({'source_container': source_container}))
        context.set_custom_status('Document Processing Containers Checked')
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Container Check')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Container Check'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize lists to store parent and extracted files
    parent_files = []
    extracted_files = []
    
     # Get the list of files in the source container
    try:
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': source_container, 'extension': '.pdf', 'prefix': prefix_path}))
        context.set_custom_status('Retrieved Source Files')
    except Exception as e:
        context.set_custom_status('Ingestion Failed During File Retrieval')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During File Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e


    # For each PDF file, split it into single-page chunks and save to chunks container
    try:
        split_pdf_tasks = []
        for file in files:
            # Append the file to the parent_files list
            parent_files.append(file)
            # Create a task to split the PDF file and append it to the split_pdf_tasks list
            split_pdf_tasks.append(context.call_activity_with_retry("split_pdf_files", retry_options, json.dumps({'source_container': source_container, 'chunks_container': chunks_container, 'file': file})))
        # Execute all the split PDF tasks and get the results
        split_pdf_files = yield context.task_all(split_pdf_tasks)
        # Flatten the list of split PDF files
        split_pdf_files = [item for sublist in split_pdf_files for item in sublist]

        # Convert the split PDF files from JSON strings to Python dictionaries
        pdf_chunks = [json.loads(x) for x in split_pdf_files]

    except Exception as e:
        context.set_custom_status('Ingestion Failed During PDF Chunking')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During PDF Chunking'
        # Custom logic for incorrect file type
        if 'not of type PDF' in str(e):
            status_record['status_message'] = 'Ingestion Failed During PDF Chunking: Non-PDF File Type Detected'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('PDF Chunking Completed')
    status_record['status_message'] = 'Chunking Completed'
    status_record['processing_progress'] = 0.2
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # For each PDF chunk, process it with Document Intelligence and save the results to the extracts container
    try:
        extract_pdf_tasks = []
        for pdf in pdf_chunks:
            # Append the child file to the extracted_files list
            extracted_files.append(pdf['child'])
            # Create a task to process the PDF chunk and append it to the extract_pdf_tasks list
            extract_pdf_tasks.append(context.call_activity("process_pdf_with_document_intelligence", json.dumps({'child': pdf['child'], 'parent': pdf['parent'], 'chunks_container': chunks_container, 'doc_intel_results_container': doc_intel_results_container, 'extracts_container': extract_container, 'entra_id': entra_id})))
        # Execute all the extract PDF tasks and get the results
        extracted_pdf_files = yield context.task_all(extract_pdf_tasks)

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Document Intelligence Extraction')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Document Intelligence Extraction'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Document Extraction Completion')
    status_record['status_message'] = 'Document Extraction Completion'
    status_record['processing_progress'] = 0.6
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    # For each extracted PDF file, generate embeddings and save the results
    try:
        generate_embeddings_tasks = []
        for file in extracted_pdf_files:
            # Create a task to generate embeddings for the extracted file and append it to the generate_embeddings_tasks list
            generate_embeddings_tasks.append(context.call_activity("generate_extract_embeddings", json.dumps({'extract_container': extract_container, 'file': file})))
        # Execute all the generate embeddings tasks and get the results
        processed_documents = yield context.task_all(generate_embeddings_tasks)
        
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Vectorization')
        status_record['status'] = -1
        status_record['status_message'] = 'Ingestion Failed During Vectorization'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    context.set_custom_status('Vectorization Completed')
    status_record['status_message'] = 'Vectorization Completed'
    status_record['processing_progress'] = 0.7
    status_record['status'] = 1
    yield context.call_activity("update_status_record", json.dumps(status_record))

    ###################### DATA INGESTION END ######################


    ###################### DATA INDEXING START ######################

    try:
        prefix_path = prefix_path.split('.')[0]

        # Get the list of files in the source container
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': extract_container, 'extension': '.json', 'prefix': prefix_path}))

        # Get the current index and its fields
        latest_index, fields = get_current_index(index_stem_name)

        # Use the user's provided index name rather than the latest index
        latest_index = index_name

        context.set_custom_status('Index Retrieval Complete')
        status_record['status_message'] = 'Index Retrieval Complete'

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Index Retrieval')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Index Retrieval'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e

    # Initialize list to store tasks for inserting records
    try:
        insert_tasks = []
        for file in files:
            # Create a task to insert a record for the file and append it to the insert_tasks list
            insert_tasks.append(context.call_activity_with_retry("insert_record", retry_options, json.dumps({'file': file, 'index': latest_index, 'fields': fields, 'extracts-container': extract_container, 'session_id': session_id, 'entra_id': entra_id})))
        # Execute all the insert record tasks and get the results
        insert_results = yield context.task_all(insert_tasks)
    except Exception as e:
        context.set_custom_status('Ingestion Failed During Indexing')
        status_record['status'] = 0
        status_record['status_message'] = 'Ingestion Failed During Indexing'
        status_record['error_message'] = str(e)
        status_record['processing_progress'] = 0.0
        yield context.call_activity("update_status_record", json.dumps(status_record))
        logging.error(e)
        raise e
    
    context.set_custom_status('Indexing Completed')
    status_record['status_message'] = 'Ingestion Completed'
    status_record['processing_progress'] = 1
    status_record['status'] = 10
    yield context.call_activity("update_status_record", json.dumps(status_record))

    ###################### DATA INDEXING END ######################

    ###################### INTERMEDIATE DATA DELETION START ######################

    if automatically_delete:

        try:
            source_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': source_container,  'prefix': prefix_path}))
            chunk_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': chunks_container,  'prefix': prefix_path}))
            doc_intel_result_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': doc_intel_results_container,  'prefix': prefix_path}))
            extract_files = yield context.call_activity_with_retry("delete_source_files", retry_options, json.dumps({'source_container': extract_container,  'prefix': prefix_path}))

            context.set_custom_status('Ingestion & Clean Up Completed')
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Completed'
            status_record['cleanup_status'] = 10
            yield context.call_activity("update_status_record", json.dumps(status_record))
        
        except Exception as e:
            context.set_custom_status('Data Clean Up Failed')
            status_record['cleanup_status'] = -1
            status_record['cleanup_status_message'] = 'Intermediate Data Clean Up Failed'
            status_record['cleanup_error_message'] = str(e)
            yield context.call_activity("update_status_record", json.dumps(status_record))
            logging.error(e)
            raise e

    ###################### INTERMEDIATE DATA DELETION END ######################

    # Return the list of parent files and processed documents as a JSON string
    return json.dumps({'parent_files': parent_files, 'processed_documents': processed_documents, 'indexed_documents': insert_results, 'index_name': latest_index})


@app.orchestration_trigger(context_name="context")
def index_documents_orchestrator(context):
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container name, index stem name, and prefix path from the payload
    extract_container = payload.get("extract_container")
    index_stem_name = payload.get("index_stem_name")
    prefix_path = payload.get("prefix_path")
    

    # Get the list of files in the source container
    files = yield context.call_activity("get_source_files", json.dumps({'source_container': extract_container, 'extension': '.json', 'prefix': prefix_path}))

    # Get the current index and its fields
    latest_index, fields = get_current_index(index_stem_name)

    # Initialize list to store tasks for inserting records
    insert_tasks = []
    for file in files:
        # Create a task to insert a record for the file and append it to the insert_tasks list
        insert_tasks.append(context.call_activity("insert_record", json.dumps({'file': file, 'index': latest_index, 'fields': fields, 'extracts-container': extract_container})))
    # Execute all the insert record tasks and get the results
    insert_results = yield context.task_all(insert_tasks)
    
    # Return the list of indexed documents and the index name as a JSON string
    return json.dumps({'indexed_documents': insert_results, 'index_name': latest_index})

@app.orchestration_trigger(context_name="context")
def delete_documents_orchestrator(context):
    
    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container name, index stem name, and prefix path from the payload
    source_container = payload.get("source_container")
    extract_container = payload.get("extract_container")
    index_stem_name = payload.get("index_stem_name")
    prefix_path = payload.get("prefix_path")

    chunks_container = f'{source_container}-chunks'
    doc_intel_results_container = f'{source_container}-doc-intel-results'

    prefix_path = prefix_path.split('.')[0]

    source_files = yield context.call_activity("delete_source_files", json.dumps({'source_container': source_container,  'prefix': prefix_path}))
    chunk_files = yield context.call_activity("delete_source_files", json.dumps({'source_container': chunks_container,  'prefix': prefix_path}))
    doc_intel_result_files = yield context.call_activity("delete_source_files", json.dumps({'source_container': doc_intel_results_container,  'prefix': prefix_path}))
    extract_files = yield context.call_activity("delete_source_files", json.dumps({'source_container': extract_container,  'prefix': prefix_path}))
    
    # Return the list of indexed documents and the index name as a JSON string
    return json.dumps({'deleted_source_files': source_files, 
                       'deleted_chunks': chunk_files, 
                       'deleted_doc_intel_results': doc_intel_result_files, 
                       'deleted_extract_files': extract_files})

# Activities
@app.activity_trigger(input_name="activitypayload")
def get_source_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    extension = data.get("extension")
    prefix = data.get("prefix")
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object from the BlobServiceClient
    container_client = blob_service_client.get_container_client(source_container)
    
    # List all blobs in the container that start with the specified prefix
    blobs = container_client.list_blobs(name_starts_with=prefix)

    # Initialize an empty list to store the names of the files
    files = []

    # For each blob in the container
    for blob in blobs:
        # If the blob's name ends with the specified extension
        if blob.name.lower().endswith(extension):
            # Append the blob's name to the files list
            files.append(blob.name)

    # Return the list of file names
    return files

@app.activity_trigger(input_name="activitypayload")
def delete_source_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    prefix = data.get("prefix")
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object from the BlobServiceClient
    container_client = blob_service_client.get_container_client(source_container)
    
    # List all blobs in the container that start with the specified prefix
    blobs = container_client.list_blobs(name_starts_with=prefix)

    # Initialize an empty list to store the names of the files
    files = []

    # For each blob in the container
    for blob in blobs:
        files.append(blob.name)

    for file in files:
        blob = container_client.get_blob_client(file)
        blob.delete_blob()

    # Return the list of file names
    return files

@app.activity_trigger(input_name="activitypayload")
def check_containers(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    
    chunks_container = f'{source_container}-chunks'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    try:
        blob_service_client.create_container(doc_intel_results_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(chunks_container)
    except Exception as e:
        pass

    # Return the list of file names
    return True

@app.activity_trigger(input_name="activitypayload")
def split_pdf_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, chunks container, and file name from the payload
    source_container = data.get("source_container")
    chunks_container = data.get("chunks_container")
    file = data.get("file")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object for the source and chunks containers
    source_container = blob_service_client.get_container_client(source_container)
    chunks_container = blob_service_client.get_container_client(chunks_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = source_container.get_blob_client(file)

    # Initialize an empty list to store the PDF chunks
    pdf_chunks = []

    # If the PDF file exists
    if  pdf_blob_client.exists():

        blob_data = pdf_blob_client.download_blob().readall()

        kind = filetype.guess(blob_data)

        if kind.EXTENSION != 'pdf':
            raise Exception(f'{file} is not of type PDF. Detected MIME type: {kind.EXTENSION}')

        # Create a PdfReader object for the PDF file
        pdf_reader = PdfReader(BytesIO(blob_data))

        # Get the number of pages in the PDF file
        num_pages = len(pdf_reader.pages)

        # For each page in the PDF file
        for i in range(num_pages):
            # Create a new file name for the PDF chunk
            new_file_name = file.replace('.pdf', '') + '_page_' + str(i+1) + '.pdf'

            # Create a PdfWriter object
            pdf_writer = PdfWriter()
            # Add the page to the PdfWriter object
            pdf_writer.add_page(pdf_reader.pages[i])

            # Create a BytesIO object for the output stream
            output_stream = BytesIO()
            # Write the PdfWriter object to the output stream
            pdf_writer.write(output_stream)

            # Reset the position of the output stream to the beginning
            output_stream.seek(0)

            # Get a BlobClient object for the PDF chunk
            pdf_chunk_blob_client = chunks_container.get_blob_client(blob=new_file_name)

            # Upload the PDF chunk to the chunks container
            pdf_chunk_blob_client.upload_blob(output_stream, overwrite=True)
            
            # Append the parent file name and child file name to the pdf_chunks list
            pdf_chunks.append(json.dumps({'parent': file, 'child': new_file_name}))

    # Return the list of PDF chunks
    return pdf_chunks
    
@app.activity_trigger(input_name="activitypayload")
def process_pdf_with_document_intelligence(activitypayload: str):
    """
    Process a PDF file using Document Intelligence.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    child = data.get("child")
    parent = data.get("parent")
    entra_id = data.get("entra_id")
    chunks_container = data.get("chunks_container")
    doc_intel_results_container = data.get("doc_intel_results_container")
    extracts_container = data.get("extracts_container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    # Get a ContainerClient object for the chunks, Document Intelligence results, and extracts containers
    chunks_container_client = blob_service_client.get_container_client(container=chunks_container)
    doc_intel_results_container_client = blob_service_client.get_container_client(container=doc_intel_results_container)
    extracts_container_client = blob_service_client.get_container_client(container=extracts_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = chunks_container_client.get_blob_client(blob=child)

    # Initialize a flag to indicate whether the PDF file has been processed
    processed = False

    # Create a new file name for the processed PDF file
    updated_filename = child.replace('.pdf', '.json')

    # Get a BlobClient object for the Document Intelligence results file
    doc_results_blob_client = doc_intel_results_container_client.get_blob_client(blob=updated_filename)
    # Check if the Document Intelligence results file exists
    if doc_results_blob_client.exists():

        # Get a BlobClient object for the extracts file
        extract_blob_client = extracts_container_client.get_blob_client(blob=updated_filename)

        # If the extracts file exists
        if extract_blob_client.exists():

            # Download the PDF file as a stream
            pdf_stream_downloader = (pdf_blob_client.download_blob())

            # Calculate the MD5 hash of the PDF file
            md5_hash = hashlib.md5()
            for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
                md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()

            # Load the extracts file as a JSON string
            extract_data = json.loads((extract_blob_client.download_blob().readall()).decode('utf-8'))

            # If the checksum in the extracts file matches the checksum of the PDF file
            if 'checksum' in extract_data.keys():
                if extract_data['checksum']==checksum:
                    # Set the processed flag to True
                    processed = True

    # If the PDF file has not been processed
    if not processed:
        # Extract the PDF file with AFR, save the AFR results, and save the extract results

        # Download the PDF file
        pdf_data = pdf_blob_client.download_blob().readall()
        # Analyze the PDF file with Document Intelligence
        doc_intel_result = analyze_pdf(pdf_data)

        # Get a BlobClient object for the Document Intelligence results file
        doc_intel_result_client = doc_intel_results_container_client.get_blob_client(updated_filename)

        # Upload the Document Intelligence results to the Document Intelligence results container
        doc_intel_result_client.upload_blob(json.dumps(doc_intel_result), overwrite=True)

        # Extract the results from the Document Intelligence results
        page_map = extract_results(doc_intel_result, updated_filename)

        # Extract the page number from the child file name
        page_number = child.split('_')[-1]
        page_number = page_number.replace('.pdf', '')
        # Get the content from the page map
        content = page_map[0][1]

        # Generate a unique ID for the record
        id_str = child + entra_id
        hash_object = hashlib.sha256()
        hash_object.update(id_str.encode('utf-8'))
        id = hash_object.hexdigest()

        # Download the PDF file as a stream
        pdf_stream_downloader = (pdf_blob_client.download_blob())

        # Calculate the MD5 hash of the PDF file
        md5_hash = hashlib.md5()
        for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
            md5_hash.update(byte_block)
        checksum = md5_hash.hexdigest()

        # Create a record for the PDF file
        record = {
            'content': content,
            'sourcefile': parent,
            'sourcepage': child,
            'pagenumber': page_number,
            'category': 'manual',
            'id': str(id),
            'checksum': checksum
        }

        # Get a BlobClient object for the extracts file
        extract_blob_client = extracts_container_client.get_blob_client(blob=updated_filename)

        # Upload the record to the extracts container
        extract_blob_client.upload_blob(json.dumps(record), overwrite=True)

    # Return the updated file name
    return updated_filename



@app.activity_trigger(input_name="activitypayload")
def generate_extract_embeddings(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the extract container and file name from the payload
    extract_container = data.get("extract_container")
    file = data.get("file")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extract container
    extract_container_client = blob_service_client.get_container_client(container=extract_container)

    # Get a BlobClient object for the extract file
    extract_blob = extract_container_client.get_blob_client(blob=file)

    # Load the extract file as a JSON string
    extract_data =  json.loads((extract_blob.download_blob().readall()).decode('utf-8'))

    # If the extract data does not contain embeddings
    if 'embeddings' not in extract_data.keys():

        # Extract the content from the extract data
        content = extract_data['content']

        # Generate embeddings for the content
        embeddings = generate_embeddings(content)

        # Update the extract data with the embeddings
        updated_record = extract_data
        updated_record['embeddings'] = embeddings

        # Upload the updated extract data to the extract container
        extract_blob.upload_blob(json.dumps(updated_record), overwrite=True)

    # Return the file name
    return file

@app.activity_trigger(input_name="activitypayload")
def insert_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the file name, index, fields, and extracts container from the payload
    file = data.get("file")
    index = data.get("index")
    fields = data.get("fields")
    extracts_container = data.get("extracts-container")
    entra_id = data.get("entra_id")
    session_id = data.get("session_id")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the extracts container
    container_client = blob_service_client.get_container_client(container=extracts_container)

    # Get a BlobClient object for the file
    blob_client = container_client.get_blob_client(blob=file)

    # Download the file as a string
    file_data = (blob_client.download_blob().readall()).decode('utf-8')

    # Load the file data as a JSON string
    file_data =  json.loads(file_data)

    # Filter the file data to only include the specified fields
    file_data = {key: value for key, value in file_data.items() if key in fields}
    file_data['entra_id'] = entra_id
    file_data['session_id'] = session_id

    # Insert the file data into the specified index
    insert_documents_vector([file_data], index)

    # Return the file name
    return file

# Standalone Functions

# This function creates a new index
@app.route(route="create_new_index", auth_level=func.AuthLevel.FUNCTION)
def create_new_index(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the JSON payload from the request
    data = req.get_json()
    # Extract the index stem name and fields from the payload
    stem_name = data.get("index_stem_name")
    # fields = data.get("fields")

    fields = {
        "content": "string", "pagenumber": "int", "sourcefile": "string", "sourcepage": "string", "category": "string",
        "entra_id": "string", "session_id": "string"
    }

    # Call the function to create a vector index with the specified stem name and fields
    response = create_vector_index(stem_name, fields)

    # Return the response
    return response


@app.route(route="get_active_index", auth_level=func.AuthLevel.FUNCTION)
def get_active_index(req: func.HttpRequest) -> func.HttpResponse:
    # Get the JSON payload from the request
    data = req.get_json()
    # Extract the index stem name from the payload
    stem_name = data.get("index_stem_name")
    
    # Call the function to get the current index for the specified stem name
    latest_index, fields = get_current_index(stem_name)

    return latest_index


@app.function_name(name="schedule_create_index")
@app.schedule(schedule="0 0 12/12 * * *", arg_name="createtimer", run_on_startup=False,
              use_monitor=False) 
def schedule_create_index(createtimer: func.TimerRequest) -> None:
    # Extract the index stem name and fields from the payload
    stem_name = 'rag-index'
   
    fields = {
        "content": "string", "pagenumber": "int", "sourcefile": "string", 
        "sourcepage": "string", "category": "string",
        "entra_id": "string", "session_id": "string"
    }

    max_retries = 3
    attempts = 0

    while attempts < max_retries:
        try:
            response = create_vector_index(stem_name, fields)
            break
        except Exception as e:
            print(f"Create Index - Attempt {attempts+1} failed: {e}")
            time.sleep(10)
            attempts += 1
            if attempts == max_retries:
                logging.error(e)


@app.function_name(name="schedule_delete_index")
@app.schedule(schedule="0 0 12/12 * * *", arg_name="deletetimer", run_on_startup=False,
              use_monitor=False) 
def schedule_delete_index(deletetimer: func.TimerRequest) -> None:
    # Extract the index stem name and fields from the payload
    stem_name = 'rag-index'
    
    max_retries = 3
    attempts = 0

    while attempts < max_retries:
        try:
            deleted_indexes = delete_indexes(stem_name, 60*24)
            break
        except Exception as e:
            print(f"Delete Indexes - Attempt {attempts+1} failed: {e}")
            time.sleep(10)
            attempts += 1
            if attempts == max_retries:
                logging.error(e)


@app.activity_trigger(input_name="activitypayload")
def update_status_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    cosmos_container = os.environ['COSMOS_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    response = container.upsert_item(data)
    return True

@app.activity_trigger(input_name="activitypayload")
def get_status_record(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    cosmos_id = data.get("cosmos_id")
    entra_id = data.get("entra_id")
    cosmos_container = os.environ['COSMOS_CONTAINER']
    cosmos_database = os.environ['COSMOS_DATABASE']
    cosmos_endpoint = os.environ['COSMOS_ENDPOINT']
    cosmos_key = os.environ['COSMOS_KEY']

    client = CosmosClient(cosmos_endpoint, cosmos_key)

    # Select the database
    database = client.get_database_client(cosmos_database)

    # Select the container
    container = database.get_container_client(cosmos_container)

    response = container.read_item(item=cosmos_id, partition_key=entra_id)
    if type(response) == dict:
        return response
    return json.loads(response)