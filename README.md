![CD Pipeline](https://github.com/anujaingale41-lab/pyspark-dataops-pipeline/actions/workflows/cd.yml/badge.svg)


# PySpark DataOps Pipeline

A PySpark batch ETL pipeline with CI/CD using GitHub Actions.  
This project demonstrates a production-like data engineering workflow: reading raw data, transforming it with PySpark, validating with unit tests, and using GitHub Actions for CI (tests + lint) and CD (packaging releases).


## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Quickstart — Step by Step](#quickstart--step-by-step)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Tests](#tests)
- [CI/CD (GitHub Actions)](#cicd-github-actions)
- [Development Tips](#development-tips)
- [License](#license)


## Project Overview
**PySpark DataOps Pipeline** is a lightweight but realistic PySpark batch ETL job.  
It reads raw CSVs, performs cleaning and transformation steps, aggregates results, and writes output as Parquet.  
The project includes unit tests and GitHub Actions workflows for CI and CD — no Docker or cloud required.

The goal is to simulate real DataOps workflow while keeping everything easy to run locally.


## Features
- PySpark batch ETL job  
- Clean, modular transformation functions  
- Local-run script (`run_job.py`)  
- Unit tests using `pytest`  
- Linting with `flake8`  
- GitHub Actions CI: run tests & lint  
- GitHub Actions CD: package and upload artifact on tag release  
- Sample CSV dataset for experimentation  


## Quickstart — Step by Step

### 1. Create virtual environment
```bash
python -m venv .venv
````

Activate it:

* PowerShell: `.venv\Scripts\Activate.ps1`
* CMD: `.venv\Scripts\activate.bat`

### 2. Install dependencies

Create `requirements.txt` and install:

```
pyspark==3.4.1
pytest==7.4.0
flake8==6.1.0
pandas==2.2.2
```

Then:

```bash
pip install -r requirements.txt
```

### 3. Create project folders

```
spark_job/
  ├── __init__.py
  ├── job.py
  └── transformations.py
tests/
  └── test_transformations.py
data/
  └── sales_raw.csv
run_job.py
.github/workflows/ci.yml
.github/workflows/cd.yml
```

### 4. Initialize Git & push

```bash
git init
git branch -M main
git add .
git commit -m "Initial project setup"
git remote add origin https://github.com/<your-username>/pyspark-dataops-pipeline.git
git push -u origin main
```


## Project Structure

```
pyspark-dataops-pipeline/
├── spark_job/
│   ├── __init__.py
│   ├── job.py
│   └── transformations.py
├── data/
│   └── sales_raw.csv
├── output/        # gitignored
├── tests/
│   └── test_transformations.py
├── run_job.py
├── requirements.txt
└── .github/
    └── workflows/
        ├── ci.yml
        └── cd.yml
```


## How It Works

1. `run_job.py` calls `run_job()` from `spark_job/job.py`
2. `job.py`:

   * Creates local SparkSession
   * Reads CSV
   * Cleans data
   * Adds derived columns
   * Aggregates and writes Parquet
3. Transformations are pure functions inside `transformations.py`, fully unit-tested
4. Tests use local SparkSession (`local[*]`)


## Tests

Run all tests:

```bash
pytest -q
```

Tests cover:

* Data cleaning
* Type conversions
* Aggregation logic
* Column addition


## CI/CD (GitHub Actions)

### CI workflow (`ci.yml`)

Runs on every push:

* Install deps
* Run PySpark tests
* Lint with flake8

### CD workflow (`cd.yml`)

Triggers on tag releases:

* Runs CI steps
* Packages PySpark job into a ZIP
* Uploads release artifact

No cloud services or secrets required.


## Development Tips

* Keep `transformations.py` pure (no SparkSession inside).
* Use `job.py` only for orchestration + Spark I/O.
* Use small input CSVs for fast testing.
* Add `output/` and `.venv/` to `.gitignore`.
