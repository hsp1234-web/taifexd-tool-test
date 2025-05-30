import unittest
import os
import json
import sys

# Add the parent directory (root of the repository) to the Python path
# so that the Taifexdtool module can be imported.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Taifexdtool import load_config

class TestConfigLoading(unittest.TestCase):
    def setUp(self):
        """Ran before each test. Backs up existing config.json if present."""
        self.config_path = "config.json"
        self.config_backup_path = self.config_path + ".backup"
        if os.path.exists(self.config_path):
            os.rename(self.config_path, self.config_backup_path)

    def tearDown(self):
        """Ran after each test. Restores backup or removes created config.json."""
        if os.path.exists(self.config_backup_path):
            os.rename(self.config_backup_path, self.config_path)
        elif os.path.exists(self.config_path):
            # If a test created a config.json and there was no backup, remove it.
            os.remove(self.config_path)

    def test_default_config_creation(self):
        """Test that a default config.json is created if none exists."""
        # Ensure no config.json exists (setUp should handle if one was there)
        if os.path.exists(self.config_path): # Should not exist due to setUp
             os.remove(self.config_path)

        config = load_config(self.config_path)
        
        self.assertTrue(os.path.exists(self.config_path), "config.json should be created by load_config.")
        
        # Check for default keys
        self.assertIn("database_path", config)
        self.assertIn("log_file", config)
        self.assertIn("log_level", config)
        self.assertIn("download_urls", config)
        
        # Check default values (optional, but good)
        self.assertEqual(config["database_path"], "taifex_data.sqlite")
        self.assertEqual(config["log_level"], "INFO")

        # tearDown will remove this created config.json

    def test_existing_config_loading(self):
        """Test loading an existing config.json with specific values."""
        test_values = {
            "database_path": "test_data.sqlite",
            "log_file": "test_tool.log",
            "log_level": "DEBUG",
            "download_urls": ["http://example.com/test.csv"]
        }
        with open(self.config_path, 'w') as f:
            json.dump(test_values, f, indent=2)
            
        config = load_config(self.config_path)
        
        self.assertEqual(config["database_path"], test_values["database_path"])
        self.assertEqual(config["log_file"], test_values["log_file"])
        self.assertEqual(config["log_level"], test_values["log_level"])
        self.assertEqual(config["download_urls"], test_values["download_urls"])

        # tearDown will remove this test config.json

    def test_malformed_config_handling(self):
        """Test handling of a malformed config.json file."""
        # Create a malformed JSON file
        with open(self.config_path, 'w') as f:
            f.write("{'database_path': 'test.db', 'log_level': 'DEBUG',") # Invalid JSON
        
        # load_config calls exit(1) on JSONDecodeError, which raises SystemExit
        with self.assertRaises(SystemExit) as cm:
            load_config(self.config_path)
        
        self.assertEqual(cm.exception.code, 1, "Should exit with code 1 on malformed JSON.")
        
        # Check if a default config is created or if the malformed one persists
        # The current behavior of load_config is to exit before creating a default one
        # if a malformed one is found. So, the malformed one might still be there.
        # tearDown will handle removing it.

if __name__ == '__main__':
    # This allows running the tests directly from this file
    unittest.main()
