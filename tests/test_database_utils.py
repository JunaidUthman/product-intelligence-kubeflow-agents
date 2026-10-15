import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'agents')))

from unittest.mock import MagicMock
for mod in ['pymysql', 'dotenv']:
    try:
        __import__(mod)
    except ImportError:
        sys.modules[mod] = MagicMock()

from database_utils import get_db_config

class TestDatabaseUtils(unittest.TestCase):
    def test_get_db_config_structure(self):
        os.environ["MYSQL_HOST"] = "test-host"
        os.environ["MYSQL_USER"] = "test-user"
        os.environ["MYSQL_PASSWORD"] = "test-pass"
        os.environ["MYSQL_DATABASE"] = "test-db"

        config = get_db_config()
        self.assertIsInstance(config, dict)
        self.assertEqual(config["host"], "test-host")
        self.assertEqual(config["user"], "test-user")
        self.assertEqual(config["password"], "test-pass")
        self.assertEqual(config["database"], "test-db")
        self.assertEqual(config["charset"], "utf8mb4")

if __name__ == '__main__':
    unittest.main()

