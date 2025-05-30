import unittest
import os
import csv
import sys

# Add the parent directory (root of the repository) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from test import apply_template_to_csv, DATA_RECOGNITION_TEMPLATES # Import necessary components

class TestApplyTemplate(unittest.TestCase):

    def setUp(self):
        """Create a temporary CSV file for testing."""
        self.test_csv_filename = "test_input.csv"
        # Ensure tests run in the same directory as the test file itself,
        # so test_input.csv is created where expected.
        self.test_dir = os.path.dirname(os.path.abspath(__file__))
        self.test_csv_path = os.path.join(self.test_dir, self.test_csv_filename)

        self.csv_content = [
            ["Col1", "Col2", "Col3"],
            ["Data1", "Data2", "Data3"],
            ["Data4", "Data5", "Data6"]
        ]
        with open(self.test_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(self.csv_content)

        # Find a standard template (assuming one exists with default comma/utf-8)
        self.standard_template = next(
            (t for t in DATA_RECOGNITION_TEMPLATES if t['parser_params']['delimiter'] == ',' and t['parser_params']['encoding'] == 'utf-8' and t['parser_params']['skiprows'] == 0),
            None # Default to None if not found, though test might fail earlier
        )
        if self.standard_template is None:
            # Fallback to a manually defined one if not found in global list, just in case
            self.standard_template = {
                "template_name": "Test Standard", 
                "parser_params": {"delimiter": ",", "skiprows": 0, "encoding": "utf-8"}
            }


    def tearDown(self):
        """Remove the temporary CSV file."""
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)

    def test_successful_parse_standard_csv(self):
        """Test successful parsing with a standard CSV template."""
        result = apply_template_to_csv(self.test_csv_path, self.standard_template)
        
        self.assertTrue(result['success'], f"Parsing should succeed. Error: {result['error_message']}")
        # The current apply_template_to_csv reads all lines after skip.
        # If skiprows is 0, it reads header + data.
        self.assertEqual(result['rows_read'], 3, "Should read header + 2 data rows.")
        self.assertIsNotNone(result['sample_data'], "Sample data should not be None.")
        self.assertEqual(len(result['sample_data']), 3, "Sample data should contain 3 rows.")
        self.assertEqual(result['sample_data'][0], self.csv_content[0]) # Header
        self.assertEqual(result['sample_data'][1], self.csv_content[1]) # First data row
        self.assertIsNone(result['error_message'], "Error message should be None for successful parse.")

    def test_parse_with_delimiter_mismatch(self):
        """Test parsing when template delimiter doesn't match CSV."""
        mismatch_template = {
            "template_name": "Semicolon Delimiter Test",
            "parser_params": {"delimiter": ";", "skiprows": 0, "encoding": "utf-8"}
        }
        result = apply_template_to_csv(self.test_csv_path, mismatch_template)
        
        self.assertTrue(result['success'], "Parsing should still be 'successful' (no exception) but data might be wrong.")
        self.assertEqual(result['rows_read'], 3, "Should read all rows, even if incorrectly delimited.")
        self.assertTrue(len(result['sample_data']) > 0, "Sample data should be present.")
        # Each row will be read as a single field because the delimiter ';' is not found
        self.assertEqual(len(result['sample_data'][0]), 1, "Header should be read as a single field due to delimiter mismatch.")
        self.assertEqual(result['sample_data'][0][0], ",".join(self.csv_content[0])) # "Col1,Col2,Col3"

    def test_parse_with_skip_rows(self):
        """Test parsing with skiprows parameter."""
        skip_template = {
            "template_name": "Skip 1 Row Test",
            "parser_params": {"delimiter": ",", "skiprows": 1, "encoding": "utf-8"}
        }
        result = apply_template_to_csv(self.test_csv_path, skip_template)
        
        self.assertTrue(result['success'], f"Parsing should succeed. Error: {result['error_message']}")
        self.assertEqual(result['rows_read'], 2, "Should read 2 data rows after skipping 1 header row.")
        self.assertEqual(len(result['sample_data']), 2, "Sample data should contain 2 rows.")
        self.assertEqual(result['sample_data'][0], self.csv_content[1]) # First data row (after header)
        self.assertEqual(result['sample_data'][1], self.csv_content[2]) # Second data row

    def test_parse_file_not_found(self):
        """Test parsing a non-existent file."""
        result = apply_template_to_csv("non_existent_file.csv", self.standard_template)
        
        self.assertFalse(result['success'], "Parsing should fail for a non-existent file.")
        self.assertIsNotNone(result['error_message'], "Error message should be present.")
        self.assertIn("File not found", result['error_message'], "Error message should indicate file not found.")
        self.assertEqual(len(result['sample_data']), 0, "Sample data should be empty on failure.")

    def test_parse_bad_encoding(self):
        """Test parsing with an incorrect encoding."""
        bad_encoding_template = {
            "template_name": "Bad Encoding Test (UTF-16)",
            "parser_params": {"delimiter": ",", "skiprows": 0, "encoding": "utf-16"}
        }
        result = apply_template_to_csv(self.test_csv_path, bad_encoding_template)
        
        self.assertFalse(result['success'], "Parsing should fail due to encoding mismatch.")
        self.assertIsNotNone(result['error_message'], "Error message should be present for encoding error.")
        # Making the check more flexible for different Python versions or specific error messages
        error_msg_lower = result['error_message'].lower()
        self.assertTrue("encoding error" in error_msg_lower or "decode" in error_msg_lower or "utf-16" in error_msg_lower or "bom" in error_msg_lower,
                        f"Error message '{result['error_message']}' should indicate an encoding issue.")

if __name__ == '__main__':
    unittest.main()
