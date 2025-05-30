#!/usr/bin/env python3
"""
Taifexdtool.py - Main script for TAIFEX Data Processing.

This script automates the downloading (simulated), parsing, transformation (placeholder),
and database storage of data files, initially designed with TAIFEX CSVs in mind.
It is configured via a `config.json` file and logs its operations.
It also generates a summary report of processed files.
"""
import json
import os
import logging
import csv
import sqlite3

def load_config(config_path="config.json"):
    """
    Loads configuration from a JSON file.

    If the specified configuration file doesn't exist, it creates a new one
    with default settings. Handles potential JSON decoding errors or file not
    found issues by printing an error and exiting.

    Args:
        config_path (str, optional): The path to the configuration file.
                                     Defaults to "config.json".

    Returns:
        dict: A dictionary containing the configuration settings.
              Exits the program if loading or creation fails critically.
    """
    default_config = {
        "database_path": "taifex_data.sqlite",  # Default path for the SQLite DB
        "log_file": "taifexdtool.log",
        "log_level": "INFO",
        "download_urls": []
    }
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            print(f"'{config_path}' not found. Created a default configuration file.")
            return default_config
    except (json.JSONDecodeError, FileNotFoundError) as e:
        # Use logging once it's configured, or print for now if critical
        print(f"Critical error loading or creating configuration: {e}. Exiting.") # Ensure this prints if logger not ready
        exit(1) # Critical to have config

def parse_csv_data(csv_content_string, delimiter=','):
    """
    Parses a CSV string, extracts a header row, and data rows.
    It cleans cell values by stripping leading/trailing whitespace.

    Args:
        csv_content_string (str): The string content of the CSV file.
        delimiter (str, optional): The delimiter used in the CSV. Defaults to ','.

    Returns:
        dict: A dictionary with two keys:
              "header" (list): A list of strings for the cleaned header row.
                               Returns empty list if no header found or on error.
              "rows" (list of lists): A list where each inner list is a data row
                                      with cleaned string values. Returns empty list
                                      if no data rows found or on error.
    """
    logger = logging.getLogger(__name__)
    if not csv_content_string or not csv_content_string.strip():
        logger.error("Cannot parse CSV: input string is empty or contains only whitespace.")
        return {"header": [], "rows": []}

    lines = csv_content_string.strip().splitlines()
    if not lines:
        logger.error("Cannot parse CSV: no lines found after stripping.")
        return {"header": [], "rows": []}
    
    try:
        # Attempt to find the first non-empty line as header
        header_index = -1
        for i, line in enumerate(lines):
            if line.strip(): # Check if the line is not just whitespace
                header_index = i
                break
        
        if header_index == -1: # All lines are empty or whitespace
            logger.error("Cannot parse CSV: all lines are empty or whitespace.")
            return {"header": [], "rows": []}

        # Use csv.reader for robust CSV parsing
        # The header line itself
        header_line_content = lines[header_index]
        # csv.reader expects an iterable of lines, so pass the single header line as a list
        header_reader = csv.reader([header_line_content], delimiter=delimiter)
        # Clean header cells
        header = [cell.strip() for cell in next(header_reader)]

        data_rows = []
        # Process subsequent lines as data
        if len(lines) > header_index + 1:
            data_line_iterable = lines[header_index+1:]
            # Filter out completely empty lines from data rows before parsing
            non_empty_data_lines = [line for line in data_line_iterable if line.strip()]
            if non_empty_data_lines:
                data_reader = csv.reader(non_empty_data_lines, delimiter=delimiter)
                for row in data_reader:
                    # Ensure all cells are processed, even if they are empty strings after parsing
                    cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
                    # Only add row if it's not entirely empty after stripping
                    if any(cleaned_row): # check if there is any non-empty string in cleaned_row
                         data_rows.append(cleaned_row)
            
        logger.info(f"Successfully parsed CSV data. Header: {header}. Number of data rows: {len(data_rows)}")
        return {"header": header, "rows": data_rows}

    except csv.Error as e:
        logger.error(f"Error parsing CSV data: {e}")
        return {"header": [], "rows": []}
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error during CSV parsing: {e}")
        return {"header": [], "rows": []}

def recognize_data_type(file_path, header, first_data_row):
    """
    Placeholder function to recognize the data type of a CSV file.

    This function is intended to be expanded with logic to distinguish different
    CSV structures (e.g., specific TAIFEX report formats) based on the file path,
    header content, and the first data row.

    Args:
        file_path (str): The path or identifier of the source file.
        header (list): A list of strings representing the header row.
        first_data_row (list): A list of strings representing the first data row.

    Returns:
        str: A string indicating the recognized data type (e.g., "generic_csv").
             Currently returns a placeholder value.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Attempting to recognize data type for file: '{file_path}'")
    logger.debug(f"Header for type recognition: {header}")
    logger.debug(f"First data row for type recognition: {first_data_row}")
    
    # Placeholder logic: In the future, this will inspect header and row content
    # to determine specific Taifex CSV structures.
    data_type = "generic_csv" 
    logger.info(f"Recognized data type for '{file_path}' as: '{data_type}'")
    return data_type

def transform_data(data_type, parsed_data):
    """
    Placeholder function to transform parsed data based on its recognized type.

    This function will later be updated to apply specific data cleaning,
    type conversion, or structural transformations based on the `data_type`.

    Args:
        data_type (str): The type of data, as determined by `recognize_data_type`.
        parsed_data (dict): The dictionary containing "header" and "rows" from
                            `parse_csv_data`.

    Returns:
        dict: The (potentially) transformed data, in the same structure as `parsed_data`.
              Currently, it returns the data as is (passthrough).
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Attempting to transform data of type: '{data_type}'")
    logger.debug(f"Data for transformation (first 2 rows): Header: {parsed_data.get('header')}, Rows: {parsed_data.get('rows', [])[:2]}")

    # Placeholder logic: Future implementations will modify parsed_data based on data_type.
    # For now, it returns the data as is.
    transformed_data = parsed_data 
    logger.info(f"Data transformation for type '{data_type}' complete (currently a passthrough).")
    return transformed_data

def generate_summary_report(processed_files_summary):
    """
    Logs a summary report of all files processed during the script's execution.

    The report includes the status for each file (Success/Failed), number of rows
    parsed and inserted, and any error messages for failed files. It also provides
    an overall summary count.

    Args:
        processed_files_summary (list): A list of dictionaries, where each
                                        dictionary contains processing summary
                                        for a single file. Expected keys include
                                        'source', 'status', 'rows_parsed',
                                        'rows_inserted', 'error_message'.
    """
    logger = logging.getLogger(__name__)
    logger.info("--- Processing Summary Report ---")
    
    total_files = len(processed_files_summary)
    successful_files = 0
    failed_files = 0
    
    for summary in processed_files_summary:
        logger.info(f"Source: {summary['source']}")
        logger.info(f"  Status: {summary['status']}")
        if summary['status'] == "Success":
            successful_files += 1
            logger.info(f"  Rows Parsed: {summary.get('rows_parsed', 'N/A')}")
            logger.info(f"  Rows Inserted: {summary.get('rows_inserted', 'N/A')}")
        else:
            failed_files += 1
            logger.error(f"  Error: {summary.get('error_message', 'No specific error message.')}")
        logger.info("-" * 30)
        
    logger.info("--- Overall Summary ---")
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successful files: {successful_files}")
    logger.info(f"Failed files: {failed_files}")
    logger.info("--- End of Report ---")

def init_db(db_path):
    """
    Initializes the SQLite database.

    Connects to the SQLite database specified by `db_path`. If the database
    does not exist, it will be created. It then ensures that the necessary
    tables (e.g., `generic_data`) are created if they don't already exist.

    Args:
        db_path (str): The file path for the SQLite database.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Initializing database at: {db_path}")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create generic_data table
        # Storing each row as a JSON object allows flexibility for varying CSV structures.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS generic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_source TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Note: Added a timestamp column for when data is inserted.
        
        conn.commit()
        logger.info("Database initialized successfully. Table 'generic_data' is ready.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error during DB initialization: {e}")
    finally:
        if conn:
            conn.close()

def insert_data(db_path, file_source, transformed_data):
    """
    Inserts transformed data into the SQLite database.

    Each row from `transformed_data` is converted into a JSON string (mapping
    header to value for that row) and stored in the `generic_data` table.

    Args:
        db_path (str): The file path for the SQLite database.
        file_source (str): Identifier for the source of the data (e.g., filename).
        transformed_data (dict): A dictionary containing "header" (list of strings)
                                 and "rows" (list of lists of strings).

    Returns:
        tuple: A tuple `(success_boolean, count_of_rows_inserted)`.
               `success_boolean` is True if all rows were processed without SQLite
               errors (even if 0 rows were inserted because the input was empty
               but valid), False otherwise.
               `count_of_rows_inserted` is the number of rows actually inserted.
    """
    logger = logging.getLogger(__name__)
    # Check for valid input structure first
    if not transformed_data or not isinstance(transformed_data, dict):
        logger.error(f"Invalid 'transformed_data' input for source '{file_source}'. Expected a dictionary.")
        return False, 0 # Critical error with input structure

    header = transformed_data.get("header")
    rows = transformed_data.get("rows")

    # If header is missing or not a list, it's an issue with data structure.
    if not header or not isinstance(header, list):
        logger.warning(f"Cannot insert data for '{file_source}': Header is missing or not a list.")
        return False, 0 
    
    # If rows key is missing or its value is not a list, it's an issue.
    if rows is None or not isinstance(rows, list): # Explicitly check for None or wrong type
        logger.warning(f"Cannot insert data for '{file_source}': Rows are missing or not a list.")
        return False, 0 
        
    # If header is present, and rows is an empty list, it's not an error; 0 rows inserted.
    if not rows: 
        logger.info(f"No data rows to insert for source '{file_source}' (header was present, rows list empty).")
        return True, 0 # Successfully "inserted" zero rows.

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        header = transformed_data["header"]
        rows_inserted_count = 0
        
        for idx, row_values in enumerate(transformed_data["rows"]):
            if len(header) != len(row_values):
                logger.warning(f"Skipping row {idx} for source '{file_source}': mismatch between header length ({len(header)}) and row length ({len(row_values)}). Row data: {row_values}")
                continue

            row_dict = dict(zip(header, row_values))
            try:
                data_json_string = json.dumps(row_dict)
            except TypeError as te:
                logger.error(f"Could not serialize row to JSON for source '{file_source}', row {idx}: {te}. Row data: {row_dict}")
                continue # Skip this row

            cursor.execute("""
                INSERT INTO generic_data (file_source, row_number, data_json) 
                VALUES (?, ?, ?)
            """, (file_source, idx, data_json_string))
            rows_inserted_count +=1
            
        conn.commit()
        if rows_inserted_count > 0:
            logger.info(f"Successfully inserted {rows_inserted_count} rows from '{file_source}' into '{db_path}'.")
        elif not transformed_data["rows"]: # No rows to begin with
             logger.info(f"No data rows were present in transformed_data for '{file_source}', so nothing to insert.")
        else: # Rows were present but all were skipped
            logger.warning(f"No rows were successfully inserted for '{file_source}' (all rows may have been skipped due to errors).")

        # This return was causing the TypeError. The logic is correctly handled by the returns after the 'finally' block.
        # return rows_inserted_count > 0 or not transformed_data["rows"] 
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error during data insertion for '{file_source}': {e}")
        if conn:
            conn.rollback()
        return False, 0
    except Exception as ex: # Catch other potential errors like issues with zip or loops
        logger.error(f"An unexpected error occurred during data insertion for '{file_source}': {ex}")
        if conn:
            conn.rollback()
        return False, 0
    finally:
        if conn:
            conn.close()
            
    # Return tuple: (success_boolean, count_of_rows_inserted)
    # Success is true if rows were inserted OR if there were no rows to insert and no errors occurred.
    # This logic might need refinement based on how "success" is defined when no rows are present.
    # For now, if no rows to insert, it's a "success" in terms of DB operation not failing.
    if rows_inserted_count > 0:
        return True, rows_inserted_count
    elif not transformed_data["rows"]: # No data rows to insert
        return True, 0 
    else: # Rows were present, but none were inserted (e.g., all skipped)
        return False, 0


def download_data(url_or_path):
    """
    Simulates downloading data from a URL or reading from a local file path.

    - If `url_or_path` starts with 'http://' or 'https://', it logs that
      actual URL downloading is not yet implemented and returns `None`.
    - Otherwise, it treats `url_or_path` as a local file path.
      - If the file exists, its text content is read and returned.
      - If the file does not exist or an IOError occurs, an error is logged
        and `None` is returned.

    Args:
        url_or_path (str): The URL or local file path to fetch data from.

    Returns:
        str or None: The content of the file as a string if successful,
                     otherwise None.
    """
    logger = logging.getLogger(__name__) # Get logger instance

    if url_or_path.startswith('http://') or url_or_path.startswith('https://'):
        # Placeholder for actual web download logic
        logger.info(f"Actual downloading from URL '{url_or_path}' is not yet implemented.")
        return None
    else: # Local file path
        if os.path.exists(url_or_path):
            try:
                with open(url_or_path, 'r') as f:
                    content = f.read()
                logger.info(f"Successfully 'downloaded' (read) data from local file: {url_or_path}")
                return content
            except IOError as e:
                logger.error(f"IOError when reading local file '{url_or_path}': {e}")
                return None
        else:
            logger.error(f"Local file not found: {url_or_path}")
            return None

def main():
    """
    Main execution function for Taifexdtool.

    Orchestrates the entire process:
    1. Loads configuration.
    2. Sets up logging.
    3. Initializes the database.
    4. Processes each data source specified in the configuration (or default test sources).
       For each source:
       a. Downloads/reads data.
       b. Parses CSV data (if applicable).
       c. Recognizes data type (placeholder).
       d. Transforms data (placeholder).
       e. Inserts data into the database.
    5. Generates a summary report of all processing activities.
    """
    # Load application configuration
    config = load_config()
    db_path = config.get("database_path", "taifex_data.sqlite") # Get db_path from config
    processing_summary_list = [] # Initialize list to store summary of each processed file

    # --- Setup Logging ---
    log_level_str = config.get("log_level", "INFO").upper()
    log_file = config.get("log_file", "taifexdtool.log")
    numeric_log_level = getattr(logging, log_level_str, logging.INFO)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create handlers
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(numeric_log_level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_log_level) # Or a different level for console

    # Get root logger and add handlers
    # It's better to configure the root logger or a specific app logger
    # For simplicity here, configuring the root logger.
    # If using a specific logger via logging.getLogger(__name__),
    # then add handlers to that specific logger.
    # For this script structure, configuring a logger instance and passing it around
    # or using a global app logger might be cleaner than configuring root.
    # However, basicConfig is often used for simple scripts.
    # Let's use basicConfig for simplicity as per prompt, but acknowledge its implications.

    logging.basicConfig(
        level=numeric_log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler() # Outputs to console
        ]
    )
    # Re-fetch the logger for the current module after basicConfig
    logger = logging.getLogger(__name__)

    logger.info(f"Configuration loaded: {config}")

    # --- Initialize Database ---
    init_db(db_path) # Ensures DB is ready before processing starts.

    # --- Determine sources to process ---
    download_sources = config.get("download_urls", [])
    if not download_sources: # If empty in config, use default test files.
        logger.info("'download_urls' is empty in config. Using default test files for demonstration.")
        download_sources = ["sample_data.csv", "http://example.com/nonexistent.csv", "non_existent_file.txt", "empty_file.csv"]

    # Create an empty CSV file for testing header-only CSV case.
    # Note: This is for demonstration/testing within this script. 
    # In a production environment, test files would typically be part of a dedicated test suite.
    empty_csv_test_file = "empty_file.csv"
    if empty_csv_test_file in download_sources: # Only create if it's in the list to be processed
        try:
            with open(empty_csv_test_file, "w") as f:
                f.write("HeaderA,HeaderB\n") # Minimal CSV with only a header
            logger.info(f"Created '{empty_csv_test_file}' for testing header-only CSV processing.")
        except IOError as e:
            logger.error(f"Could not create '{empty_csv_test_file}' for testing: {e}")
            # The script will proceed; download_data will handle the file not being found if creation failed.

    # --- Main processing loop for each source ---
    for source in download_sources:
        # Initialize summary for the current source. Default to failure.
        current_file_summary = {
            'source': source,
            'status': 'Failed', 
            'downloaded_content_size': 0,
            'rows_parsed': 0,
            'rows_inserted': 0,
            'error_message': 'Processing did not start or was interrupted early.'
        }
        logger.info(f"--- Starting processing for source: {source} ---")
        
        # 1. Download Data
        data_content = download_data(source)
        if data_content is None:
            # download_data logs specific reasons (file not found, URL not implemented).
            # Capture a general error for summary.
            if source.startswith('http'):
                current_file_summary['error_message'] = f"Download failed: URL download not implemented or unreachable."
            else: # Local file
                current_file_summary['error_message'] = f"Download failed: File not found at '{source}' or unreadable."
            logger.warning(f"Download failed for '{source}'. {current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- Finished processing for source: {source} (Download Failed) ---")
            continue # Move to the next source

        current_file_summary['downloaded_content_size'] = len(data_content)
        logger.info(f"Successfully downloaded/read from '{source}'. Size: {len(data_content)} bytes.")

        # 2. Process only if it's a CSV file
        if not source.endswith(".csv"):
            current_file_summary['status'] = 'Skipped'
            current_file_summary['error_message'] = "Source is not a CSV file."
            logger.info(f"Skipping CSV processing for non-CSV source: {source}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- Finished processing for source: {source} (Skipped Non-CSV) ---")
            continue

        # 3. Parse CSV Data
        logger.info(f"Attempting to parse CSV data from '{source}'...")
        parsed_data = parse_csv_data(data_content)
        if not parsed_data or not parsed_data.get("header"): # parse_csv_data returns {"header": [], "rows": []} on error or empty
            current_file_summary['error_message'] = "Parsing failed: No header found or CSV is invalid/empty."
            logger.warning(f"Parsing failed for '{source}'. {current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- Finished processing for source: {source} (Parsing Failed) ---")
            continue
        
        current_file_summary['rows_parsed'] = len(parsed_data.get("rows", []))
        logger.info(f"Successfully parsed CSV from '{source}'. Header: {parsed_data['header']}. Rows: {current_file_summary['rows_parsed']}.")

        # If no data rows after parsing (e.g. header-only CSV), it's a "success" for this stage.
        if not parsed_data.get("rows"):
            current_file_summary['status'] = 'Success' # Processed file, no data to insert.
            current_file_summary['error_message'] = None # No error in this case.
            logger.info(f"CSV from '{source}' has a header but no data rows to process further or insert.")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- Finished processing for source: {source} (Header-only CSV) ---")
            continue

        # 4. Recognize Data Type (using first data row)
        first_data_row = parsed_data["rows"][0]
        data_type = recognize_data_type(source, parsed_data["header"], first_data_row)
        # This is a placeholder, so no specific error handling for it yet.

        # 5. Transform Data
        transformed_data = transform_data(data_type, parsed_data)
        if not transformed_data: # Should ideally not happen with current passthrough logic
            current_file_summary['error_message'] = "Transformation step failed or returned no data."
            logger.warning(f"Transformation failed for '{source}'. {current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- Finished processing for source: {source} (Transformation Failed) ---")
            continue
        
        # 6. Insert Data into Database
        logger.info(f"Attempting to insert data from '{source}' into database '{db_path}'...")
        insert_success, num_inserted = insert_data(db_path, source, transformed_data)
        current_file_summary['rows_inserted'] = num_inserted
        if insert_success:
            current_file_summary['status'] = 'Success'
            current_file_summary['error_message'] = None
            logger.info(f"Database insertion for '{source}' completed. Inserted {num_inserted} rows.")
        else:
            # insert_data logs specific SQLite errors.
            current_file_summary['error_message'] = f"Database insertion failed. {num_inserted} rows inserted before error or 0 if all failed. Check logs."
            logger.error(f"Database insertion failed for '{source}'. {current_file_summary['error_message']}")
            # Status remains 'Failed'
        
        processing_summary_list.append(current_file_summary)
        logger.info(f"--- Finished processing for source: {source} ---")

    # --- Generate Final Summary Report ---
    generate_summary_report(processing_summary_list)

    # Clean up the dummy empty_file.csv if it was created
    if empty_csv_test_file in download_sources and os.path.exists(empty_csv_test_file):
        try:
            os.remove(empty_csv_test_file)
            logger.info(f"Cleaned up test file: '{empty_csv_test_file}'.")
        except OSError as e:
            logger.error(f"Error removing test file '{empty_csv_test_file}': {e}")


if __name__ == "__main__":
    main()
