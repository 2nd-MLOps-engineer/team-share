import sys
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline_common import normalize_missing_values  # noqa: E402


class NormalizeMissingValuesTest(unittest.TestCase):
    def test_normalizes_known_markers_and_trims_strings(self):
        source = pd.DataFrame(
            {
                "value": ["", " NULL ", "-", "NaN", " valid "],
                "number": ["0", "1", "2", "3", "4"],
            }
        )

        result = normalize_missing_values(source)

        self.assertIsNone(result.loc[0, "value"])
        self.assertIsNone(result.loc[1, "value"])
        self.assertIsNone(result.loc[2, "value"])
        self.assertIsNone(result.loc[3, "value"])
        self.assertEqual(result.loc[4, "value"], "valid")
        self.assertEqual(result.loc[0, "number"], "0")


if __name__ == "__main__":
    unittest.main()
