from pathlib import Path
from zipfile import ZipFile


def extract_zip(zip_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(output_dir.glob("*.csv"))
    if csv_files:
        print(f"Extracted CSV already exists: {output_dir}")
        return output_dir

    print(f"Extracting {zip_path}")
    with ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(output_dir)

    print(f"Extraction complete: {output_dir}")
    return output_dir
