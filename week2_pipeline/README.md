# Week 2 — Python ETL Pipeline

## What it does
Config-driven ETL pipeline built using OOP.
Reads CSV, cleans, transforms, writes CSV and Parquet output.

## How to run
1. Open Week2_Project.ipynb in Jupyter
2. Update the config dict with your filepath
3. Run all cells

## Config
```python
config = {
    "filepath"  : "sales_data.csv",
    "group_col" : "region",
    "value_col" : "sales",
    "agg_func"  : "sum",
    "output_dir": "output/"
}
```

## Tech stack
- Python 3.x
- pandas
- OOP (class-based pipeline)
- Error handling + logging
