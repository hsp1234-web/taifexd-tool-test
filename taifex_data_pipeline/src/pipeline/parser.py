# src/pipeline/parser.py
# -*- coding: utf-8 -*-
# Data Parser: Responsible for parsing CSV files.

import csv
import os

try:
    from src.utils.logger import get_logger
    from src.config import settings
except ImportError:
    import sys
    current_script_path = os.path.abspath(__file__)
    project_root_for_direct_run = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.utils.logger import get_logger # type: ignore
    from src.config import settings # type: ignore

logger = get_logger()

def parse_csv_file(file_path, encoding=None, delimiter=","):
    '''
    Parses a CSV file, extracts a header row, and data rows.
    Strips leading/trailing whitespace from cells.
    Args:
        file_path (str): Path to the CSV file.
        encoding (str, optional): File encoding. Defaults to settings.DEFAULT_ENCODING.
        delimiter (str, optional): CSV delimiter. Defaults to ",".
    Returns:
        dct: {"header": list_of_strings, "rows": list_of_lists_of_strings}.
              Empty lists if error or no data.
    '''
    resolved_encoding = encoding or settings.DEFAULT_ENCODING
    logger.info(f"Parsing CSV: {file_path}, Encoding: {resolved_encoding}, Delimiter: '{delimiter}'")

    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        return {"header": [], "rows": []}

    header = []
    data_rows = []

    try:
        with open(file_path, "r", encoding=resolved_encoding, newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=delimiter)

            # Find first non-empty line as header
            for row in csv_reader:
                if any(cell.strip() for cell in row):
                    header = [cell.strip() for cell in row]
                    break

            if not header:
                logger.warning(f'No header found in CSV: {file_path}')
                return {"header": [], "rows": []}

            # Read data rows
            for row in csv_reader: # Continues from line after header
                cleaned_row = [cell.strip() for cell in row]
                if any(cleaned_row): # Add if not entirely empty
                    data_rows.append(cleaned_row)

        logger.info(f"Parsed CSV: {file_path} - {len(header)} cols, {len(data_rows)} rows.")
        return {"header": header, "rows": data_rows}

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error for {file_path} with {resolved_encoding}: {e}. Try other encodings.")
        return {"header": [], "rows": []}
    except csv.Error as e:
        logger.error(f"CSV parsing error for {file_path}: {e}")
        return {"header": [], "rows": []}
    except Exception as e:
        logger.error(f'Unexpected error parsing CSV {file_path}: {e}', exc_info=True)
        return {"header": [], "rows": []}
