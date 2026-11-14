import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Set {name} in the project-root .env file")
    return value


SOURCE_DIR = Path(os.getenv("SOURCE_DATA_DIR", str(PROJECT_ROOT / "source_data"))).expanduser().resolve()
AZURE_STORAGE_ACCOUNT_URL = required_env("AZURE_STORAGE_ACCOUNT_URL")
AZURE_STORAGE_FILESYSTEM = required_env("AZURE_STORAGE_FILESYSTEM")

# Protect immutable landing: never overwrite existing files.
if os.getenv("OVERWRITE_EXISTING", "false").lower() in {"true", "yes", "1"}:
    raise ValueError("OVERWRITE_EXISTING=true is forbidden for landing ingestion")
OVERWRITE_EXISTING = False
