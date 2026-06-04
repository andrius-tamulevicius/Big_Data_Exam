from __future__ import annotations

from src.config import (
    AIS_ARCHIVE_PATH,
    AIS_DATA_URL,
    CENTER_LATITUDE,
    CENTER_LONGITUDE,
    END_DATE,
    EXTRACTED_DATA_DIR,
    FILTERED_AIS_PATH,
    RADIUS_NM,
    START_DATE,
)
from src.scripts.download_data import download_file
from src.scripts.extract_data import extract_zip
from src.scripts.filter_data import filter_invalid_records


def main() -> None:
    print("AIS collision detection pipeline")
    print(f"Timeframe: {START_DATE} to {END_DATE}")
    print(f"Dataset: {AIS_DATA_URL}")

    download_file(AIS_DATA_URL, AIS_ARCHIVE_PATH)
    extract_zip(AIS_ARCHIVE_PATH, EXTRACTED_DATA_DIR)
    filter_invalid_records(EXTRACTED_DATA_DIR, FILTERED_AIS_PATH)
    print(f"Filtered output: {FILTERED_AIS_PATH}")


if __name__ == "__main__":
    main()
