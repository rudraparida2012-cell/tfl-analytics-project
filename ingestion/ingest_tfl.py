import os
import json
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from typing import Optional


load_dotenv()

TFL_APP_KEY = os.getenv("TFL_APP_KEY")
BASE_URL = "https://api.tfl.gov.uk"

RAW_BASE_PATH = Path("data/raw")


def get_timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def save_json(data, folder_name: str, file_prefix: str) -> None:
    folder_path = RAW_BASE_PATH / folder_name
    folder_path.mkdir(parents=True, exist_ok=True)

    timestamp = get_timestamp()
    file_path = folder_path / f"{file_prefix}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved file: {file_path}")


def call_tfl_api(endpoint: str, params: Optional[dict] = None):
    if params is None:
        params = {}

    params["app_key"] = TFL_APP_KEY

    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, params=params, timeout=30)

    print(f"Calling: {response.url}")
    print(f"Status Code: {response.status_code}")

    if response.status_code != 200:
        print("Error response:", response.text)
        return None

    return response.json()


def ingest_line_status():
    data = call_tfl_api("/Line/Mode/tube/Status")
    if data is not None:
        save_json(data, "line_status", "tube_line_status")


def ingest_disruptions():
    data = call_tfl_api("/Line/Mode/tube/Disruption")
    if data is not None:
        save_json(data, "disruptions", "tube_disruptions")


def ingest_routes():
    data = call_tfl_api("/Line/Mode/tube/Route")
    if data is not None:
        save_json(data, "routes", "tube_routes")


def ingest_stoppoints():
    data = call_tfl_api("/StopPoint/Mode/tube")
    if data is not None:
        save_json(data, "stoppoints", "tube_stoppoints")


def main():
    if not TFL_APP_KEY:
        raise ValueError("TFL_APP_KEY is missing from .env")

    print("Starting TfL ingestion...")
    ingest_line_status()
    ingest_disruptions()
    ingest_routes()
    ingest_stoppoints()
    print("TfL ingestion completed.")


if __name__ == "__main__":
    main()
