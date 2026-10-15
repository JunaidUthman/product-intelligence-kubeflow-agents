import unittest
from unittest.mock import MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'agents')))

for mod in ['pymysql', 'dotenv']:
    try:
        __import__(mod)
    except ImportError:
        sys.modules[mod] = MagicMock()

from ranking_agent import calculate_category_scores

class TestRankingAgent(unittest.TestCase):
    def test_calculate_category_scores_empty(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        calculate_category_scores(mock_cursor, session_id=1)
        # Should execute select query, but no update query
        mock_cursor.execute.assert_called_once()

    def test_calculate_category_scores_logic(self):
        mock_cursor = MagicMock()
        # Mock product data returned by DB for session 1
        mock_cursor.fetchall.return_value = [
            {
                'score_entry_id': 101,
                'categorie': 'chargers',
                'prix_usd': 50.0,
                'note_etoiles': 4.5,
                'stock': 'In Stock'
            },
            {
                'score_entry_id': 102,
                'categorie': 'chargers',
                'prix_usd': 100.0,
                'note_etoiles': 4.0,
                'stock': 'Out of stock'
            }
        ]

        calculate_category_scores(mock_cursor, session_id=1)

        # Expected score calculations for chargers:
        # max_price = 100.0
        # Product 101:
        #   norm_rating = 4.5 / 5.0 = 0.90
        #   norm_price = 1.0 - (50.0 / 100.0) = 0.50
        #   stock_score = 1.0 (In Stock)
        #   final_score = (0.90 * 0.4) + (0.50 * 0.35) + (1.0 * 0.25)
        #               = 0.36 + 0.175 + 0.25 = 0.785
        # Product 102:
        #   norm_rating = 4.0 / 5.0 = 0.80
        #   norm_price = 1.0 - (100.0 / 100.0) = 0.0
        #   stock_score = 0.0 (Out of stock)
        #   final_score = (0.80 * 0.4) + 0 + 0 = 0.32

        # Verify UPDATE queries were executed
        update_calls = [call for call in mock_cursor.execute.call_args_list if "UPDATE product_scores" in call[0][0]]
        self.assertEqual(len(update_calls), 2)

        first_update_args = update_calls[0][0][1]
        self.assertAlmostEqual(first_update_args[0], 0.785, places=3)
        self.assertEqual(first_update_args[1], 101)

        second_update_args = update_calls[1][0][1]
        self.assertAlmostEqual(second_update_args[0], 0.32, places=3)
        self.assertEqual(second_update_args[1], 102)

if __name__ == '__main__':
    unittest.main()

