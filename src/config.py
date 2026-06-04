from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

START_DATE = "2021-12-01"
END_DATE = "2021-12-31"

CENTER_LATITUDE = 55.225000
CENTER_LONGITUDE = 14.245000
RADIUS_NM = 50.0

AIS_DATA_URL = "http://aisdata.ais.dk/2021/aisdk-2021-12.zip"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
AIS_ARCHIVE_PATH = RAW_DATA_DIR / "aisdk-2021-12.zip"
EXTRACTED_DATA_DIR = PROJECT_ROOT / "data" / "extracted"
FILTERED_DATA_DIR = PROJECT_ROOT / "data" / "filtered"
FILTERED_AIS_PATH = FILTERED_DATA_DIR / "aisdk_2021_12_valid"
