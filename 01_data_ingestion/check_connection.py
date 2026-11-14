from utils.azure_connection import get_adls_service_client
from utils.config import AZURE_STORAGE_FILESYSTEM
from utils.logger import get_logger

logger = get_logger(__name__)


def check_adls_connection() -> bool:
    
    """
    Checks that Entra ID authentication works and the configured filesystem
    can be accessed. The identity needs appropriate storage data-plane RBAC.
    """
    service_client = get_adls_service_client()
    try:
        filesystem_client = service_client.get_file_system_client(
            file_system=AZURE_STORAGE_FILESYSTEM
        )

        # A listing request checks access without creating or changing data.
        next(iter(filesystem_client.get_paths(path=None, recursive=False)), None)
        logger.info(
            "ADLS connection and filesystem access check succeeded: %s",
            AZURE_STORAGE_FILESYSTEM,
        )
        return True
    
    except Exception:
        logger.exception(
            "ADLS connection/access check failed for filesystem %s",
            AZURE_STORAGE_FILESYSTEM,
        )
        raise

    finally:
        service_client.close()
