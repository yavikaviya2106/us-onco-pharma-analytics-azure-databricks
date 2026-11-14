from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from utils.config import AZURE_STORAGE_ACCOUNT_URL


def get_adls_service_client() -> DataLakeServiceClient:
    """Use the Azure CLI identity already authenticated with `az login`."""
    return DataLakeServiceClient(
        account_url=AZURE_STORAGE_ACCOUNT_URL,
        credential=AzureCliCredential(),
    )
