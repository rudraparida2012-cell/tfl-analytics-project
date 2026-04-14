import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

project_root = Path(__file__).resolve().parents[1]
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")
container_name = os.getenv("AZURE_CONTAINER_RAW", "raw")

print("ENV file path:", env_path)
print("AZURE_STORAGE_ACCOUNT:", storage_account)
print("AZURE_STORAGE_KEY loaded:", storage_key is not None)
print("AZURE_CONTAINER_RAW:", container_name)

if not storage_account or not storage_key:
    raise ValueError("Azure storage account details are missing from .env")

account_url = f"https://{storage_account}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_key)
container_client = blob_service_client.get_container_client(container_name)

local_raw_path = project_root / "data" / "raw"

def upload_file(local_file: Path):
    relative_path = local_file.relative_to(local_raw_path)
    blob_path = str(relative_path).replace("\\", "/")

    with open(local_file, "rb") as data:
        container_client.upload_blob(name=blob_path, data=data, overwrite=True)

    print(f"Uploaded: {local_file} -> {blob_path}")

def main():
    if not local_raw_path.exists():
        raise ValueError(f"Local raw folder does not exist: {local_raw_path}")

    files = list(local_raw_path.rglob("*.json"))
    print(f"Found {len(files)} raw files to upload")

    for file in files:
        upload_file(file)

    print("Upload completed.")

if __name__ == "__main__":
    main()
