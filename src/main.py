from __future__ import annotations

from src.config import (
    AIS_ARCHIVE_PATH,
    AIS_DATA_URL,
    CLEAN_ALL_AIS_PATH,
    CLEAN_AREA_AIS_PATH,
    COLLISION_CANDIDATES_PATH,
    FINAL_RESULT_PATH,
    TEMP_DATA_DIR,
    TRAJECTORY_MAP_PATH,
    VALIDATED_CANDIDATES_PATH,
    VALID_BROAD_AIS_PATH,
)
from src.scripts.filter_area import filter_exact_area
from src.scripts.find_candidates import find_collision_candidates
from src.scripts.process_daily_data import process_zip_daily
from src.scripts.download_data import download_file
from src.scripts.remove_gps_jumps import remove_gps_jumps
from src.scripts.filter_candidates import filter_collision_candidates
from src.scripts.start_spark import start_spark
from src.scripts.write_outputs import write_final_outputs


def main() -> None:
    print(f"Dataset: {AIS_DATA_URL}")

    # Downloads the data if it is missing
    download_file(AIS_DATA_URL, AIS_ARCHIVE_PATH)

    # Starks a spark session
    spark = start_spark()

    try:
        # Initial data collection and cleaning steps
        process_zip_daily(spark, AIS_ARCHIVE_PATH, TEMP_DATA_DIR, VALID_BROAD_AIS_PATH)
        remove_gps_jumps(spark, VALID_BROAD_AIS_PATH, CLEAN_ALL_AIS_PATH)
        filter_exact_area(spark, CLEAN_ALL_AIS_PATH, CLEAN_AREA_AIS_PATH)

        # Finds, filters, and outputs the result
        find_collision_candidates(spark, CLEAN_AREA_AIS_PATH, COLLISION_CANDIDATES_PATH)
        filter_collision_candidates(spark, COLLISION_CANDIDATES_PATH, CLEAN_ALL_AIS_PATH, VALIDATED_CANDIDATES_PATH)
        write_final_outputs(spark, VALIDATED_CANDIDATES_PATH, CLEAN_ALL_AIS_PATH, FINAL_RESULT_PATH, TRAJECTORY_MAP_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
