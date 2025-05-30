# Taifex Data Processing Tools

This project contains Python scripts for downloading, processing, and testing data, initially designed with TAIFEX (Taiwan Futures Exchange) data in mind.

## Scripts

### 1. `Taifexdtool.py`

**Purpose:** Automates the processing of data files (e.g., CSVs from TAIFEX). It handles:
- Loading configuration from `config.json`.
- "Downloading" data (currently simulates from local files, extendable for web downloads).
- Parsing CSV files, including basic cleaning (stripping whitespace).
- Placeholder functions for future data type recognition and data transformation.
- Storing processed data (as JSON strings) into an SQLite database (`taifex_data.sqlite` by default).
- Logging all operations to a configured log file (e.g., `taifexdtool.log`) and to the console.
- Generating a summary report of all files processed during its run.

**Usage (Basic):**
```bash
python3 Taifexdtool.py
```
Configuration is managed via `config.json`. The script processes data sources listed in the `download_urls` array within `config.json`. If this array is empty or missing, it defaults to a predefined list of test sources (local files and example URLs).

### 2. `test.py`

**Purpose:** A command-line tool to test and evaluate different parsing strategies ("Data Recognition Templates") against a specific CSV file. This helps in identifying the optimal parameters for `Taifexdtool.py` or for understanding the structure of new or varied CSV formats.

**Features:**
- Defines a set of parsing templates (e.g., different delimiters, encodings, skipped rows).
- Applies each defined template to a user-specified target CSV file.
- Reports on the success (file read, basic parsing) or failure of each template.
- Provides a basic recommendation for the most suitable template(s) based on successful parsing and the number of rows read.

**Usage (Basic):**
```bash
python3 test.py <path_to_csv_file_to_test>
```
Example:
```bash
python3 test.py sample_data.csv
```
If no CSV file path is provided, it prints defined templates and a usage message.

## Setup and Dependencies

- Python 3.x
- No external libraries are strictly required for the current basic functionality beyond standard Python modules (`argparse`, `csv`, `json`, `logging`, `os`, `sqlite3`).

## Unit Tests
Unit tests are located in the `tests/` directory. They can be run using:
```bash
python3 -m unittest discover tests
```
This will execute tests for `Taifexdtool.py`'s configuration loading, logging setup, and `test.py`'s CSV parsing logic.

## Future Development
- Implement actual web downloading capabilities in `Taifexdtool.py`.
- Enhance data type recognition in `Taifexdtool.py` to identify specific TAIFEX CSV structures.
- Implement data transformation logic in `Taifexdtool.py` based on recognized data types.
- Expand parsing capabilities for more complex CSVs (e.g., handling specific Taifex headers/footers) and other potential formats (e.g., ZIP archives containing CSVs).
- Develop more sophisticated evaluation metrics and reporting in `test.py`.
- Add comprehensive integration tests.
- Refine error handling and user feedback across both scripts.
```
