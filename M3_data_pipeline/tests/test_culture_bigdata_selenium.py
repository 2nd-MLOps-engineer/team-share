import logging
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


PIPELINE_DIR = Path(__file__).resolve().parents[1]
TEST_DEPENDENCIES = PIPELINE_DIR / ".test_deps"
sys.path.insert(0, str(TEST_DEPENDENCIES))
sys.path.insert(0, str(PIPELINE_DIR))

import culture_bigdata_selenium as culture  # noqa: E402


class CultureRawLoadTest(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("culture-test")
        self.config = culture.DatabaseConfig(
            host="localhost",
            port=5432,
            dbname=culture.EXPECTED_DB_NAME,
            user="test",
            password="test",
        )

    def validation(self, name):
        return culture.CsvValidation(
            path=Path(name),
            columns=("id",),
            row_count=1,
            null_count=0,
        )

    @staticmethod
    def connection():
        connection = MagicMock()
        transaction = MagicMock()
        transaction.__enter__.return_value = None
        connection.transaction.return_value = transaction
        cursor = MagicMock()
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor
        connection.cursor.return_value = cursor_context
        return connection, cursor

    def test_multi_product_load_uses_one_transaction_and_post_commit_counts(self):
        connection, _cursor = self.connection()
        inputs = (
            (self.validation("one.csv"), "culture_one"),
            (self.validation("two.csv"), "culture_two"),
        )

        with (
            patch.object(culture, "connect_database", return_value=connection),
            patch.object(culture, "create_text_table"),
            patch.object(culture, "copy_csv_rows", side_effect=[1, 1]),
            patch.object(culture, "table_row_count", side_effect=[1] * 6),
        ):
            loads = culture.load_csv_group_to_raw(
                inputs,
                self.config,
                self.logger,
            )

        connection.transaction.assert_called_once_with()
        connection.close.assert_called_once_with()
        self.assertEqual([load.table for load in loads], ["culture_one", "culture_two"])

    def test_copy_mismatch_stops_before_final_table_replacement(self):
        connection, cursor = self.connection()
        inputs = (
            (self.validation("one.csv"), "culture_one"),
            (self.validation("two.csv"), "culture_two"),
        )

        with (
            patch.object(culture, "connect_database", return_value=connection),
            patch.object(culture, "create_text_table"),
            patch.object(culture, "copy_csv_rows", side_effect=[1, 0]),
            patch.object(culture, "table_row_count", side_effect=[1, 0]),
        ):
            with self.assertRaisesRegex(culture.AutomationError, "검증/COPY"):
                culture.load_csv_group_to_raw(inputs, self.config, self.logger)

        # schema + two advisory locks + two staging drops; final DROP is never reached.
        self.assertEqual(cursor.execute.call_count, 5)
        connection.close.assert_called_once_with()


class CultureTemporaryFileLifecycleTest(unittest.TestCase):
    def test_success_cleanup_removes_only_files_in_run_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            run_dir = Path(parent) / "run"
            run_dir.mkdir()
            csv_path = run_dir / "source.csv"
            csv_path.write_text("id\n1\n", encoding="utf-8")
            result = culture.DownloadedFile("product", "title", csv_path, 5)

            culture.remove_ephemeral_downloads(
                [result],
                run_dir,
                logging.getLogger("culture-test"),
            )

            self.assertFalse(csv_path.exists())
            self.assertFalse(run_dir.exists())

    def test_cleanup_refuses_file_outside_run_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            parent_path = Path(parent)
            run_dir = parent_path / "run"
            run_dir.mkdir()
            outside = parent_path / "source.csv"
            outside.write_text("id\n1\n", encoding="utf-8")
            result = culture.DownloadedFile("product", "title", outside, 5)

            culture.remove_ephemeral_downloads(
                [result],
                run_dir,
                logging.getLogger("culture-test"),
            )

            self.assertTrue(outside.exists())


class CultureManifestResumeTest(unittest.TestCase):
    def test_completed_product_is_skipped_on_resume(self):
        urls = culture.TARGET_URLS[:2]
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            args = SimpleNamespace(
                urls=list(urls),
                load_only=None,
                download_dir=base / "downloads",
                log_dir=base / "logs",
                purpose="business",
                other_purpose=None,
                download_timeout=30,
                headless=True,
                keep_downloads=True,
            )
            driver = MagicMock()
            downloaded = []

            def download_first(**kwargs):
                item_id = culture.product_id(kwargs["url"])
                downloaded.append(item_id)
                if len(downloaded) == 2:
                    raise culture.AutomationError("download", "temporary failure")
                path = kwargs["download_dir"] / f"{item_id}.csv"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("id\n1\n", encoding="utf-8")
                return culture.DownloadedFile(item_id, "first", path, path.stat().st_size)

            with (
                patch.object(culture, "parse_args", return_value=args),
                patch.object(culture, "load_database_config", return_value=self._config()),
                patch.object(culture, "load_credentials", return_value=("id", "pw")),
                patch.object(culture, "create_driver", return_value=driver),
                patch.object(culture, "login"),
                patch.object(culture, "download_csv_product", side_effect=download_first),
            ):
                self.assertEqual(culture.main(), 2)

            first_id = culture.product_id(urls[0])
            second_id = culture.product_id(urls[1])
            self.assertEqual(downloaded, [first_id, second_id])
            manifest_path = culture.manifest_path_for(args.download_dir, urls)
            self.assertTrue(manifest_path.exists())

            resumed_downloads = []

            def download_second(**kwargs):
                item_id = culture.product_id(kwargs["url"])
                resumed_downloads.append(item_id)
                path = kwargs["download_dir"] / f"{item_id}.csv"
                path.write_text("id\n2\n", encoding="utf-8")
                return culture.DownloadedFile(item_id, "second", path, path.stat().st_size)

            validation = culture.CsvValidation(
                path=Path("unused.csv"),
                columns=("id",),
                row_count=1,
                null_count=0,
            )
            with (
                patch.object(culture, "parse_args", return_value=args),
                patch.object(culture, "load_database_config", return_value=self._config()),
                patch.object(culture, "load_credentials", return_value=("id", "pw")),
                patch.object(culture, "create_driver", return_value=driver),
                patch.object(culture, "login"),
                patch.object(culture, "download_csv_product", side_effect=download_second),
                patch.object(culture, "validate_csv", return_value=validation),
                patch.object(culture, "load_csv_group_to_raw", return_value=[]),
                patch.object(culture, "remove_ephemeral_downloads"),
            ):
                self.assertEqual(culture.main(), 0)

            self.assertEqual(resumed_downloads, [second_id])
            self.assertFalse(manifest_path.exists())
            logger = logging.getLogger("bigdata_culture_selenium")
            for handler in logger.handlers:
                handler.close()
            logger.handlers.clear()

    @staticmethod
    def _config():
        return culture.DatabaseConfig(
            host="localhost",
            port=5432,
            dbname=culture.EXPECTED_DB_NAME,
            user="test",
            password="test",
        )


if __name__ == "__main__":
    unittest.main()
