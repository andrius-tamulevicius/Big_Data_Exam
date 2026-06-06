# Big Data Examination: Detection of Vessel Collisions

The objective of this examination is to evaluate your ability to process large-scale temporal and spatial data. You are required to identify two vessels that have collided (or experienced the closest possible physical proximity indicating a collision) within a specified marine area. You must visualize their respective trajectories 10 minutes prior to and 10 minutes following the time of collision.

This README contains the basic information about the contents of the codebase and the development and Docker set up/requirements.

**The full explanation is presented in the `report.pdf`.**

## Deliverables

The codebase contains the following:

- Report: `report.pdf` The report explains the chosen methodology and the results.
- Source code: `src/`
- Main entry point: `src/main.py`
- Configuration: `src/config.py`
- Python dependencies: `requirements.txt`
- Development setup instructions: `README.md`
- Pipeline results (detected collision information): `collision_result.json`
- Trajectory map: `collision_trajectory_map.html`

## Dataset

The dataset is set to download automatically. However you are able to download the data yourself at: http://aisdata.ais.dk/2021/aisdk-2021-12.zip

Store the downloaded dataset (the whole ZIP file) under here:

`data/raw/aisdk-2021-12.zip`

If this ZIP file already exists, the application will skip the download step and use the local file.

## Development Setup

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

The application also requires Spark. To configure follow the following steps:

On Windows, configure Hadoop native files globally:

```text
C:/Winutils/bin/winutils.exe
C:/Winutils/bin/hadoop.dll
```

Set:

```text
HADOOP_HOME=C:/Winutils
Path includes C:/Winutils/bin
```

Run the pipeline:

```bash
python -m src.main
```
