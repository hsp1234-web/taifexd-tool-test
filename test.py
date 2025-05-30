#!/usr/bin/env python3
"""
test.py - CSV Parsing Strategy Tester.

This script provides a command-line interface to test various parsing strategies
(defined as "Data Recognition Templates") against a given CSV file. It helps in
determining suitable parsing parameters (delimiter, encoding, skiprows, etc.)
for unknown or varied CSV formats.

The script applies each predefined template to the target CSV, reports on the
success or failure of parsing with that template, and provides a basic
recommendation for the best-matching template(s) based on rows read.
"""

import os
import argparse
import csv
# import json
# import logging # Future imports

# Placeholder for Data Recognition Templates
# Each dictionary represents a potential parsing strategy for a CSV file.
# These parameters would eventually be used to guide or configure
# the parsing logic (potentially by calling functions from Taifexdtool.py
# or by re-implementing a flexible parser in test.py).

DATA_RECOGNITION_TEMPLATES = [
    {
        "template_name": "Standard Comma Separated",
        "description": "Assumes a standard CSV with comma delimiter, header in the first row.",
        "parser_params": {
            "delimiter": ",",
            "skiprows": 0, # or header_row: 0
            "encoding": "utf-8",
            # Potentially other params like quotechar, escapechar, column_names, type_hints etc.
        }
    },
    {
        "template_name": "Semicolon Separated with Header",
        "description": "Assumes CSV with semicolon delimiter, header in the first row.",
        "parser_params": {
            "delimiter": ";",
            "skiprows": 0,
            "encoding": "utf-8",
        }
    },
    {
        "template_name": "Comma Separated, Skip 2 Rows",
        "description": "Assumes CSV with comma delimiter, actual data starts after skipping 2 rows.",
        "parser_params": {
            "delimiter": ",",
            "skiprows": 2,
            "encoding": "utf-8",
        }
    },
    {
        "template_name": "UTF-16 Encoded, Tab Separated",
        "description": "A more complex example with different encoding and delimiter.",
        "parser_params": {
            "delimiter": "\t", # Tab
            "skiprows": 0,
            "encoding": "utf-16",
        }
    }
]

def apply_template_to_csv(csv_filepath, template):
    """
    Applies a single parsing template to a CSV file and reports the outcome.

    It attempts to read the CSV using parameters (delimiter, encoding, skiprows)
    defined in the template. It captures basic parsing results including success/failure,
    rows read, a sample of data, and any error messages.

    Args:
        csv_filepath (str): The path to the CSV file to be tested.
        template (dict): A dictionary representing the parsing template. Expected keys:
                         'template_name' (str), 
                         'parser_params' (dict with 'delimiter', 'encoding', 'skiprows').

    Returns:
        dict: A dictionary summarizing the result of applying the template:
              - 'template_name' (str): Name of the template used.
              - 'success' (bool): True if parsing was successful (no critical errors), False otherwise.
              - 'rows_read' (int): Number of rows successfully read by csv.reader after skipping.
              - 'error_message' (str or None): Error message if parsing failed, else None.
              - 'sample_data' (list of lists): A small sample (first few rows) of the parsed data
                                             if successful, otherwise an empty list.
    """
    template_name = template.get("template_name", "Unknown Template")
    parser_params = template.get("parser_params", {})
    
    delimiter = parser_params.get("delimiter", ",")
    encoding = parser_params.get("encoding", "utf-8")
    skiprows = parser_params.get("skiprows", 0)

    # print(f"Attempting to parse '{csv_filepath}' with template: '{template_name}' (Del: '{delimiter}', Enc: '{encoding}', Skip: {skiprows})...")

    result = {
        'template_name': template_name,
        'success': False,
        'rows_read': 0,
        'error_message': None,
        'sample_data': []
    }

    try:
        with open(csv_filepath, 'r', encoding=encoding, newline='') as f:
            # Skip initial rows if specified
            for _ in range(skiprows):
                next(f) # Read and discard header/skipped lines
            
            reader = csv.reader(f, delimiter=delimiter)
            
            read_count = 0
            for i, row in enumerate(reader):
                if i < 5: # Read up to 5 data lines for sample
                    result['sample_data'].append(row)
                read_count += 1
            
            result['rows_read'] = read_count
            result['success'] = True
            # print(f"  Successfully parsed with '{template_name}'. Rows read: {read_count}.")

    except FileNotFoundError:
        result['error_message'] = f"File not found: {csv_filepath}"
        # print(f"  Error with '{template_name}': {result['error_message']}")
    except UnicodeDecodeError as e:
        result['error_message'] = f"Encoding error ({encoding}): {e}"
        # print(f"  Error with '{template_name}': {result['error_message']}")
    except csv.Error as e:
        result['error_message'] = f"CSV parsing error: {e}"
        # print(f"  Error with '{template_name}': {result['error_message']}")
    except LookupError as e: # For invalid encoding names
        result['error_message'] = f"Invalid encoding name ('{encoding}'): {e}"
        # print(f"  Error with '{template_name}': {result['error_message']}")
    except StopIteration: # Handles empty file after skipping rows
        result['error_message'] = f"No lines to read in '{csv_filepath}' after skipping {skiprows} rows."
        # This might be success depending on expectation, but for now, let's flag it if no data rows read
        if skiprows > 0 : result['success'] = True # If we skipped rows and then it's empty, that's fine.
        # print(f"  Note for '{template_name}': {result['error_message']}")
    except Exception as e:
        result['error_message'] = f"An unexpected error occurred: {e}"
        # print(f"  Error with '{template_name}': {result['error_message']}")
        
    return result

def run_tests_on_csv(csv_filepath, templates):
    """
    Runs all defined parsing templates against a single CSV file.

    Args:
        csv_filepath (str): The path to the CSV file to be tested.
        templates (list): A list of template dictionaries (e.g., DATA_RECOGNITION_TEMPLATES).

    Returns:
        list: A list of result dictionaries, where each dictionary is the output
              from `apply_template_to_csv` for one template.
    """
    print(f"\nRunning all parsing tests for: {csv_filepath}")
    results = []
    for template in templates:
        test_result = apply_template_to_csv(csv_filepath, template)
        results.append(test_result)
    return results

def generate_test_report(csv_filepath, test_results):
    """
    Generates and prints a formatted test report to the console.

    The report includes results for each template applied and a recommendations
    section suggesting potentially suitable templates based on successful parsing
    and the number of rows read.

    Args:
        csv_filepath (str): Path to the CSV file that was tested.
        test_results (list): A list of result dictionaries from `run_tests_on_csv`.
    """
    print(f"\n--- Test Report for: {csv_filepath} ---")

    successful_templates = [] # Store templates that successfully read data rows

    for result in test_results:
        status = "SUCCESS" if result['success'] else "FAILED"
        print(f"\nTemplate: {result['template_name']}")
        print(f"  Status: {status}")

        if result['success']:
            print(f"  Rows Read: {result['rows_read']}")
            if result['sample_data']:
                # Displaying only the first row of sample data for brevity.
                # Limits to first 5 fields, each field up to 30 chars.
                sample_row_display = [str(field)[:30] for field in result['sample_data'][0][:5]]
                print(f"  Sample Data (first row, up to 5 fields, 30 chars/field): {sample_row_display}")
            elif result['rows_read'] > 0:
                # This case might occur if sample_data was empty but rows_read > 0,
                # though current logic in apply_template_to_csv should populate sample_data if rows_read > 0.
                print("  Sample Data: Not captured in detail (but rows were read).")
            else: # rows_read == 0 but success == True (e.g., header-only CSV, or empty file after skip)
                print("  Sample Data: No data rows were present or read.")
            
            # Collect templates that successfully read at least one data row for recommendations
            if result['rows_read'] > 0: 
                successful_templates.append({
                    "name": result['template_name'],
                    "rows_read": result['rows_read']
                })
        else:
            print(f"  Error: {result['error_message']}") # Display error message for failed templates
    
    # --- Recommendations Section ---
    print("\n--- Recommendations ---")
    if successful_templates:
        print("Potentially suitable templates (those that successfully parsed >0 data rows):")
        
        # Sort successful templates by rows_read in descending order to find best candidates
        successful_templates.sort(key=lambda x: x['rows_read'], reverse=True)
        
        for tpl_info in successful_templates:
            print(f"  - {tpl_info['name']} (read {tpl_info['rows_read']} rows)")

        # Highlight the template(s) that read the most rows
        if successful_templates: # Ensure list is not empty after filtering
            max_rows = successful_templates[0]['rows_read']
            # Find all templates that achieved this max_rows count
            best_candidates = [tpl['name'] for tpl in successful_templates if tpl['rows_read'] == max_rows]
            
            if len(best_candidates) == 1:
                print(f"\nBest candidate based on most rows read: {best_candidates[0]} ({max_rows} rows)")
            else: # Multiple templates share the max row count
                print(f"\nBest candidates (all read {max_rows} data rows):")
                for candidate_name in best_candidates:
                    print(f"  - {candidate_name}")
    else:
        # This message is printed if no template resulted in success AND rows_read > 0
        print("No suitable template found that successfully parsed actual data rows from the file.")
    
    print("\n--- End of Report ---")


def main():
    """
    Main entry point for the CSV parsing strategy tester script.

    Parses command-line arguments (expects an optional CSV file path),
    displays defined parsing templates, and if a valid CSV file is provided,
    runs all templates against it and generates a test report.
    """
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(description="Test framework for Taifexdtool CSV parsing and processing strategies.")
    parser.add_argument(
        "csv_file", 
        nargs='?', # Makes the argument optional
        default=None, # Default value if no argument is provided
        help="Path to the CSV file to test. (Optional)"
    )
    # Example of how other arguments could be added in the future:
    # parser.add_argument("-t", "--template", help="Specify a parsing template/strategy name to run exclusively.")
    # parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose (DEBUG level) output for this script.")
    
    args = parser.parse_args()

    print("`test.py` - TAIFEX Data Processing Configuration Tester - Initial Setup")

    # Display information about the provided CSV file argument
    if args.csv_file:
        print(f"CSV file specified for testing: {args.csv_file}")
        if not os.path.exists(args.csv_file):
            # This warning is also helpful if the user mistypes the path.
            print(f"Warning: Specified CSV file does not exist: {args.csv_file}")
    else:
        print("No CSV file specified for testing in this run.")

    # Display defined templates for user information, regardless of CSV input
    if DATA_RECOGNITION_TEMPLATES:
        print("\nDefined Data Recognition Templates:")
        for template in DATA_RECOGNITION_TEMPLATES:
            print(f"  - {template['template_name']}: {template['description']}")
        print("-" * 30) # Visual separator
    
    # Proceed with testing if a CSV file path is provided and the file actually exists
    if args.csv_file:
        if os.path.exists(args.csv_file):
            # Run all defined templates against the specified CSV file
            results = run_tests_on_csv(args.csv_file, DATA_RECOGNITION_TEMPLATES)
            # Generate and print the formatted report based on the results
            generate_test_report(args.csv_file, results)
        else:
            # If file doesn't exist, reiterate that tests cannot be run.
            print(f"\nCannot run tests: CSV file '{args.csv_file}' does not exist.")
    else:
        # If no CSV file was specified on the command line, guide the user.
        print("\nNo CSV file provided. To run tests, please specify a CSV file path as an argument.")


if __name__ == "__main__":
    main()
