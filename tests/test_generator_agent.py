import unittest
import sys
import os

# Add src and src/agents to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'agents')))

from generator_agent import get_scraping_targets

class TestGeneratorAgent(unittest.TestCase):
    def test_targets_structure(self):
        targets = get_scraping_targets()
        self.assertIsInstance(targets, list)
        self.assertGreater(len(targets), 0, "Targets list should not be empty")

        for target in targets:
            self.assertIn("nom_boutique", target)
            self.assertIn("url", target)
            self.assertIn("platform", target)
            self.assertIn("category", target)

            # Check types
            self.assertIsInstance(target["nom_boutique"], str)
            self.assertIsInstance(target["url"], str)
            self.assertIsInstance(target["platform"], str)
            self.assertIsInstance(target["category"], str)

            # Check valid URL
            self.assertTrue(target["url"].startswith("http://") or target["url"].startswith("https://"),
                            f"Invalid URL: {target['url']}")

if __name__ == '__main__':
    unittest.main()

