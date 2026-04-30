from dotenv import load_dotenv
import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeBatchDocumentsRequest, AzureBlobContentSource
from azure.core.exceptions import HttpResponseError


def analyze_batch_documents():
    load_dotenv()  # Load environment variables from .env file

    endpoint = os.getenv("DOCUMENTINTELLIGENCE_ENDPOINT")
    key = os.getenv("DOCUMENTINTELLIGENCE_API_KEY")
    output_container_sas_url = os.getenv("OUTPUT_CONTAINER_SAS_URL")
    input_container_sas_url = os.getenv(
        "INPUT_CONTAINER_SAS_URL")

    # Create a DocumentIntelligenceClient using the endpoint and API key
    document_intelligence_client = DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key))

    request = AnalyzeBatchDocumentsRequest(
        result_container_url=output_container_sas_url,
        azure_blob_source=AzureBlobContentSource(
            container_url=input_container_sas_url,
        ),
    )

    poller = document_intelligence_client.begin_analyze_batch_documents(
        model_id="prebuilt-invoice",
        body=request,
    )

    print("Analyzing batch documents...")
    final_result = poller.result()
    print("Batch analyze completed.")
    print(f"Succeeded count: {final_result.succeeded_count}")
    print(f"Failed count: {final_result.failed_count}")
    print(f"Skipped count: {final_result.skipped_count}")


if __name__ == "__main__":
    try:
        analyze_batch_documents()
    except HttpResponseError as error:
        if error.error is not None:
            if error.error.code == "InvalidImage":
                print(f"Received an invalid image error: {error.error}")
            if error.error.code == "InvalidRequest":
                print(f"Received an invalid request error: {error.error}")
            raise
        # If the inner error is None and then it is possible to check the message to get more information
        if "Invalid request".casefold() in error.message.casefold():
            print(f"Uh-oh! Seems there was an invalid request: {error}")
        raise
