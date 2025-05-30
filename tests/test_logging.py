import unittest
import os
import json
import logging
import sys
import tempfile # For creating temporary log files

# Add the parent directory (root of the repository) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Taifexdtool import load_config, main as taifex_main # main might be too broad, let's see

class TestLoggingSetup(unittest.TestCase):
    
    def setUp(self):
        """Set up for logging tests."""
        self.test_dir = tempfile.mkdtemp() # Create a temporary directory for test artifacts
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir) # Change CWD to temp dir to isolate file creation

        self.config_path = "test_config.json"
        self.log_file_name = "test_app.log"
        
        # Create a specific config for this test
        self.test_config_values = {
            "database_path": "test_data.sqlite",
            "log_file": self.log_file_name, # Log to a file within the temp dir
            "log_level": "DEBUG", # Use DEBUG for more verbose test logging if needed
            "download_urls": []
        }
        with open(self.config_path, 'w') as f:
            json.dump(self.test_config_values, f)

        # Ensure Taifexdtool uses our test config for its logging setup.
        # This requires load_config to be called with our specific config path.
        # The main() function in Taifexdtool.py calls load_config() without arguments.
        # To test logging setup, we need to replicate or carefully invoke it.
        # For now, we'll try to replicate the logging setup part of main().

    def tearDown(self):
        """Clean up after logging tests."""
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        
        log_file_path = os.path.join(self.test_dir, self.log_file_name) # Path within temp dir
        if os.path.exists(log_file_path): # Check if it exists before removing
             os.remove(log_file_path)

        os.chdir(self.original_cwd) # Restore original CWD
        if os.path.exists(self.test_dir): # Clean up the temporary directory itself
             try:
                 os.rmdir(self.test_dir) # Fails if not empty
             except OSError:
                 # If rmdir fails, try to remove files first then rmdir
                 for f in os.listdir(self.test_dir):
                     os.remove(os.path.join(self.test_dir, f))
                 os.rmdir(self.test_dir)


    def test_log_file_creation_and_message(self):
        """Test that the log file is created and a test message is written."""
        
        # Replicate logging setup from Taifexdtool.main() using our test_config
        config = load_config(config_path=self.config_path) # Load our specific test config
        
        log_level_str = config.get("log_level", "INFO").upper()
        log_file_from_config = config.get("log_file", "default_tool.log") # Use the one from our config
        
        numeric_log_level = getattr(logging, log_level_str, logging.INFO)

        # Crucially, ensure handlers from previous test runs (if any) are cleared,
        # especially for the root logger if basicConfig was used.
        # For a more isolated test, configure a specific logger instance, not root.
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers[:]: # Iterate over a copy
                root_logger.removeHandler(handler)
                handler.close() # Important to close file handlers

        # Setup logging using basicConfig (as in Taifexdtool.py)
        # Log file path will be relative to CWD, which is now self.test_dir
        logging.basicConfig(
            level=numeric_log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file_from_config), 
                logging.StreamHandler(sys.stdout) # So we can see logs during test run
            ]
        )
        
        logger = logging.getLogger("TestLogger") # Get a logger instance
        test_message = "This is a test log message for test_log_file_creation_and_message."
        logger.info(test_message)

        # Flush handlers to ensure message is written to file
        for handler in logging.getLogger().handlers:
            handler.flush()
            if isinstance(handler, logging.FileHandler): # Close file handler to ensure write
                handler.close()


        log_file_path = os.path.join(self.test_dir, self.log_file_name)
        self.assertTrue(os.path.exists(log_file_path), f"Log file '{log_file_path}' should be created.")
        
        with open(log_file_path, 'r') as lf:
            log_content = lf.read()
            
        self.assertIn(test_message, log_content, "Test message should be in the log file content.")

if __name__ == '__main__':
    unittest.main()
