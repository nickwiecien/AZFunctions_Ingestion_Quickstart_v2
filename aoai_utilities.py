import datetime
from pathlib import Path
import time
import json
import openai
import os


def generate_embeddings(text):
    """
    Generates embeddings for the given text using the specified embeddings model provided by OpenAI.

    Args:
        text (str): The text to generate embeddings for.

    Returns:
        embeddings (list): The embeddings generated for the given text.
    """

    # Configure OpenAI with Azure settings
    openai.api_type = "azure"
    openai.api_base = os.environ['AOAI_ENDPOINT']
    openai.api_version = "2023-03-15-preview"
    openai.api_key = os.environ['AOAI_KEY']

    # Initialize variable to track if the embeddings have been processed
    processed = False
    # Attempt to generate embeddings, retrying on failure
    while not processed:
        try:
            # Make API call to OpenAI to generate embeddings
            response = openai.Embedding.create(input=text, engine=os.environ['AOAI_EMBEDDINGS_MODEL'])
            processed = True
        except Exception as e:  # Catch any exceptions and retry after a delay
            print(e)

            # Added to handle exception where passed context exceeds embedding model's context window
            if 'maximum context length' in str(e):
                text = text[:int(len(text)*0.95)]

            time.sleep(5)

    # Extract embeddings from the response
    embeddings = response['data'][0]['embedding']
    return embeddings

def get_transcription(filename):
    """
    Transcribes the given audio file using the specified transcription model provided by OpenAI.

    Args:
        filename (str): The path to the audio file to transcribe.

    Returns:
        transcript (str): The transcription of the audio file.
    """

    # Configure OpenAI with Azure settings
    openai.api_type = "azure"
    openai.api_base = os.environ['AOAI_WHISPER_ENDPOINT']
    openai.api_key = os.environ['AOAI_WHISPER_KEY']
    openai.api_version = "2023-09-01-preview"

    # Specify the model and deployment ID for the transcription
    model_name = "whisper-1"
    deployment_id =  os.environ['AOAI_WHISPER_MODEL']

    # Specify the language of the audio
    audio_language="en"

    # Initialize an empty string to store the transcript
    transcript = ''

    # Initialize variable to track if the audio has been transcribed
    transcribed = False

    # Attempt to transcribe the audio, retrying on failure
    while not transcribed:
        try:
            result = openai.Audio.transcribe(
                file=open(filename, "rb"),            
                model=model_name,
                deployment_id=deployment_id
            )
            transcript = result.text
            transcribed = True
        except Exception as e:  # Catch any exceptions and retry after a delay
            print(e)
            time.sleep(10)
            pass

    # If a transcript was generated, return it
    if len(transcript)>0:
        return transcript

    # If no transcript was generated, raise an exception
    raise Exception("No transcript generated")