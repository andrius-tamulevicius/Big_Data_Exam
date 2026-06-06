from pathlib import Path
from zipfile import ZipFile

from pyspark.sql import SparkSession

from src.scripts.filter_data import read_filter_daily_csv
from src.scripts.manage_stages import finish_stage, prepare_stage


def process_zip_daily(spark: SparkSession, zip_path: Path, temp_dir: Path, output_path: Path) -> None:
    if not prepare_stage(output_path):
        return

    temp_dir.mkdir(parents=True, exist_ok=True)

    # Process one CSV from the ZIP at a time
    with ZipFile(zip_path, "r") as zip_file:
        csv_names = sorted(name for name in zip_file.namelist() if name.lower().endswith(".csv"))

        for csv_name in csv_names:
            print(f"Processing {csv_name}")
            extracted_path = Path(zip_file.extract(csv_name, temp_dir))

            try:
                # Filters the day and appends it to Parquet dataset for later usee
                filtered = read_filter_daily_csv(spark, extracted_path)
                filtered.write.mode("append").parquet(str(output_path))
            finally:
                if extracted_path.exists():
                    extracted_path.unlink()

    finish_stage(output_path)
    print(f"Daily filtered output: {output_path}")
