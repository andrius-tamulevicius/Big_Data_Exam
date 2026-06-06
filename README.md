# Big_Data_Exam

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
