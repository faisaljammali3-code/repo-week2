
# Offline-First ETL & EDA Pipeline (Week 2)

A robust, reproducible data pipeline built to process sales data, handle data quality issues, and generate analytics-ready datasets. This project follows modern data engineering practices including idempotency, "fail-fast" data quality checks, and separation of concerns.

## Features
*   **Reproducible ETL:** A single script (`run_etl.py`) processes raw CSVs into optimized Parquet files.
*   **Data Quality Checks:** Validates schema, keys, and missing values before processing.
*   **Safe Transformations:** Prevents join explosions and handles timezone-aware datetimes correctly.
*   **Reporting:** Generates automated run metadata (JSON) and insightful EDA visualizations.

## Project Structure
```text
├── data/
│   ├── raw/             # Immutable input CSVs (orders.csv, users.csv)
│         
├── notebooks/           # EDA analysis (reads from processed data)
├── reports/
│   ├── figures/         # Exported PNG charts from EDA
│   └── summary.md       # Executive summary of findings
├── scripts/             # Entry points (run_etl.py)
├── src/                 # Source code (etl, io, quality, joins, transforms)
├── pyproject.toml       # Project configuration
└── requirements.txt     # Pinned dependencies
```

## Setup & Installation

This project uses `uv` for fast package management.

1.  **Create Virtual Environment:**
    ```bash
    uv venv
    ```

2.  **Install Dependencies:**
    ```bash
    # Install required libraries (pandas, plotly, etc.)
    uv pip install -r requirements.txt
    
    # Install the project in editable mode (Required for local imports)
    uv pip install -e .
    ```

## How to Run

### 1. Run the ETL Pipeline
This command runs the full pipeline: loading data, performing quality checks, cleaning, joining, and writing outputs.

```bash
# uv run handles the environment automatically
uv run scripts/run_etl.py
```
*After running, check `data/processed/_run_meta.json` to see the execution stats.*

### 2. Run EDA
Open `notebooks/eda.ipynb` in VS Code or Jupyter Lab.
*   Ensure you select the kernel associated with your `.venv`.
*   Run all cells to generate charts and analysis.

## Outputs
After running the pipeline, the following artifacts are generated:
*   `data/processed/analytics_table.parquet`: The final joined table for analysis.
*   `data/processed/orders_clean.parquet`: Cleaned orders data.
*   `reports/figures/`: Exported charts (Revenue Trend, Refund Rates, etc.).
*   `reports/summary.md`: A business summary of the key findings.
```
## Proof of Execution
Here is a screenshot showing the generated artifacts and reports:

![Submission Proof]
(submission_proof.png)