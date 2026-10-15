import unittest
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'agents')))

from unittest.mock import MagicMock
# If running locally without dependencies installed, mock external modules
for mod in ['pymysql', 'dotenv']:
    try:
        __import__(mod)
    except ImportError:
        sys.modules[mod] = MagicMock()

from processor_agent import normalize_price_to_usd, clean_and_aggregate_data

class TestProcessorAgent(unittest.TestCase):
    def test_normalize_price_empty_or_none(self):
        self.assertEqual(normalize_price_to_usd(""), 0.0)
        self.assertEqual(normalize_price_to_usd(None), 0.0)
        self.assertEqual(normalize_price_to_usd("Free"), 0.0)

    def test_normalize_price_usd(self):
        self.assertEqual(normalize_price_to_usd("$29.99"), 29.99)
        self.assertEqual(normalize_price_to_usd("49.00 USD"), 49.00)

    def test_normalize_price_eur(self):
        # 100 * 1.08 = 108.0
        self.assertEqual(normalize_price_to_usd("100 €"), 108.0)
        self.assertEqual(normalize_price_to_usd("50 EUR"), 54.0)

    def test_normalize_price_gbp(self):
        # 100 * 1.25 = 125.0
        self.assertEqual(normalize_price_to_usd("100 £"), 125.0)
        self.assertEqual(normalize_price_to_usd("20 GBP"), 25.0)

    def test_clean_and_aggregate_missing_data_dir(self):
        old_env = os.environ.get("DATA_DIR")
        if "DATA_DIR" in os.environ:
            del os.environ["DATA_DIR"]
        try:
            result = clean_and_aggregate_data()
            self.assertEqual(result, [])
        finally:
            if old_env is not None:
                os.environ["DATA_DIR"] = old_env

    def test_clean_and_aggregate_valid_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sample_data = [
                {
                    "boutique": "TestShop",
                    "categorie": "chargers",
                    "nom": "Fast Charger 65W",
                    "prix": "29.99 $",
                    "lien": "https://example.com/item1",
                    "stock": "In Stock"
                },
                # Duplicate item by lien to test deduplication
                {
                    "boutique": "TestShop",
                    "categorie": "chargers",
                    "nom": "Fast Charger 65W Duplicate",
                    "prix": "29.99 $",
                    "lien": "https://example.com/item1",
                    "stock": "In Stock"
                }
            ]
            file_path = os.path.join(temp_dir, "test_products.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(sample_data, f)

            old_env = os.environ.get("DATA_DIR")
            os.environ["DATA_DIR"] = temp_dir
            try:
                aggregated = clean_and_aggregate_data()
                self.assertEqual(len(aggregated), 1)
                item = aggregated[0]
                self.assertEqual(item["nom"], "Fast Charger 65W")
                self.assertEqual(item["prix_usd"], 29.99)
                self.assertEqual(item["boutique"], "TestShop")
            finally:
                if old_env is not None:
                    os.environ["DATA_DIR"] = old_env
                else:
                    del os.environ["DATA_DIR"]

if __name__ == '__main__':
    unittest.main()
