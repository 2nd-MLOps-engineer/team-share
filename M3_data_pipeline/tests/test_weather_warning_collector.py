import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))


class WeatherWarningCollectorTest(unittest.TestCase):
    def load_collector(self):
        collector_path = PIPELINE_DIR / "collector" / "weather_warning.py"
        spec = importlib.util.spec_from_file_location(
            "weather_warning_collector_under_test",
            collector_path,
        )
        module = importlib.util.module_from_spec(spec)
        with (
            patch.object(Path, "is_file", return_value=True),
            patch("dotenv.load_dotenv", return_value=True),
            patch.dict(os.environ, {"WEATHER_WARNING_API_KEY": "test-key"}),
        ):
            spec.loader.exec_module(module)
        return module

    def test_main_builds_current_status_snapshot_before_atomic_replace(self):
        collector = self.load_collector()
        warning_raw = pd.DataFrame({"stnId": ["108"]})
        status_raw = pd.DataFrame({"tmFc": ["202609281200"]})
        current_raw = pd.DataFrame({"t6": ["강풍주의보"]})

        collector.collect_warning_list = MagicMock(return_value=warning_raw)
        collector.build_warning_raw = MagicMock(return_value=warning_raw)
        collector.collect_warning_status = MagicMock(return_value=status_raw)
        collector.build_status_current_raw = MagicMock(return_value=current_raw)
        collector.replace_warning_raw_group = MagicMock()

        collector.main()

        collector.build_status_current_raw.assert_called_once_with(status_raw)
        collector.replace_warning_raw_group.assert_called_once_with(
            warning_raw,
            current_raw,
        )


if __name__ == "__main__":
    unittest.main()
