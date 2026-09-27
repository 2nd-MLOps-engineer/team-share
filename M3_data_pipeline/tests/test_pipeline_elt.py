import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pipeline_elt as common  # noqa: E402
from dataset_processors import normalize_nulls  # noqa: E402
from pipeline_elt import (  # noqa: E402
    STAGING_IDENTIFIER_MAX_LENGTH,
    _atomic_replace_processed_group,
    _make_staging_identifier,
    replace_raw_dataset_group,
    validate_durunubi_relations,
)
from pipeline_metadata import (  # noqa: E402
    DatasetSpec,
    IndexSpec,
    SourceKind,
    TableSpec,
)


class NormalizeNullsTest(unittest.TestCase):
    def test_normalizes_known_markers_and_trims_strings(self):
        source = pd.DataFrame(
            {
                "value": ["", " NULL ", "-", "NaN", " valid "],
                "number": ["0", "1", "2", "3", "4"],
            }
        )

        result = normalize_nulls(source)

        self.assertTrue(pd.isna(result.loc[0, "value"]))
        self.assertTrue(pd.isna(result.loc[1, "value"]))
        self.assertTrue(pd.isna(result.loc[2, "value"]))
        self.assertTrue(pd.isna(result.loc[3, "value"]))
        self.assertEqual(result.loc[4, "value"], "valid")
        self.assertEqual(result.loc[0, "number"], "0")


class StagingIdentifierTest(unittest.TestCase):
    def test_short_processed_staging_identifier(self):
        name = _make_staging_identifier(
            "air_quality",
            "processed",
        )

        self.assertLessEqual(
            len(name),
            STAGING_IDENTIFIER_MAX_LENGTH,
        )
        self.assertTrue(
            name.startswith("stg_air_quality_processed_")
        )

    def test_long_processed_staging_identifier(self):
        name = _make_staging_identifier(
            "culture_sports_facility_nearby_public_transport",
            "processed",
        )

        self.assertEqual(
            len(name),
            STAGING_IDENTIFIER_MAX_LENGTH,
        )
        self.assertIn("_processed_", name)

    def test_long_raw_staging_identifier(self):
        name = _make_staging_identifier(
            "culture_sports_facility_nearby_public_transport",
            "raw",
        )

        self.assertEqual(
            len(name),
            STAGING_IDENTIFIER_MAX_LENGTH,
        )
        self.assertIn("_raw_", name)

    def test_staging_identifiers_are_unique(self):
        table = (
            "culture_sports_facility_"
            "nearby_public_transport"
        )

        first = _make_staging_identifier(
            table,
            "processed",
        )
        second = _make_staging_identifier(
            table,
            "processed",
        )

        self.assertNotEqual(first, second)


class AtomicDatasetReplacementTest(unittest.TestCase):
    def test_multi_table_raw_replacement_shares_one_transaction(self):
        raw_frames = {
            "parent": pd.DataFrame({"id": ["1"]}),
            "child": pd.DataFrame({"id": ["10"], "parent_id": ["1"]}),
        }

        connection = MagicMock()
        connection.execute.return_value = Mock(
            scalar_one=Mock(return_value=1)
        )
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction

        with patch.object(pd.DataFrame, "to_sql") as to_sql:
            replace_raw_dataset_group(
                raw_frames,
                engine,
            )

        engine.begin.assert_called_once_with()
        self.assertEqual(to_sql.call_count, 2)
        sql = "\n".join(
            str(call.args[0])
            for call in connection.execute.call_args_list
        )
        self.assertIn('DROP TABLE IF EXISTS "raw"."parent"', sql)
        self.assertIn('DROP TABLE IF EXISTS "raw"."child"', sql)

    def test_constraint_columns_are_validated_before_database_writes(self):
        processed = {"items": pd.DataFrame({"id": ["1"]})}
        engine = MagicMock()

        with self.assertRaisesRegex(ValueError, "PRIMARY KEY 컬럼 누락"):
            _atomic_replace_processed_group(
                engine,
                processed,
                primary_keys={"items": ("missing_id",)},
            )

        engine.begin.assert_not_called()


class GenericRawDatasetLoadTest(unittest.TestCase):
    def test_multi_table_raw_dataset_is_processed_and_replaced_as_one_group(self):
        specs = (
            TableSpec(
                table="trails",
                processor="trails",
                source_kind=SourceKind.RAW_DATABASE,
                primary_key=("route_id",),
            ),
            TableSpec(
                table="segments",
                processor="segments",
                source_kind=SourceKind.RAW_DATABASE,
                primary_key=("segment_id",),
                indexes=(IndexSpec("idx_segments_route", ("route_id",)),),
            ),
        )
        dataset = DatasetSpec(
            name="routes",
            collector_script="collector/routes.py",
            default_cron="0 0 * * *",
            tables=specs,
        )
        raw_frames = {
            "trails": pd.DataFrame({"route_id": ["R1"]}),
            "segments": pd.DataFrame(
                {"segment_id": ["S1"], "route_id": ["R1"]}
            ),
        }
        processed_frames = {
            table: frame.copy()
            for table, frame in raw_frames.items()
        }
        expected_results = {
            "trails": (1, 0),
            "segments": (1, 0),
        }
        expected_dq = {"trails": Mock(), "segments": Mock()}

        connection_context = MagicMock()
        connection_context.__enter__.return_value = MagicMock()
        engine = MagicMock()
        engine.connect.return_value = connection_context

        with (
            self.assertLogs(common.LOGGER, level="INFO") as logs,
            patch.object(
                common.pd,
                "read_sql_query",
                side_effect=[raw_frames["trails"], raw_frames["segments"]],
            ) as read_sql,
            patch.object(
                common,
                "_process_dataset_frames",
                return_value=(processed_frames, expected_results, expected_dq),
            ) as process_frames,
            patch.object(
                common,
                "_atomic_replace_processed_group",
            ) as replace_group,
        ):
            result = common.load_dataset_from_raw(dataset, engine)

        self.assertEqual(result, (expected_results, expected_dq))
        messages = "\n".join(logs.output)
        self.assertIn("stage=source_read status=OK elapsed_seconds=", messages)
        self.assertIn("stage=process status=OK elapsed_seconds=", messages)
        self.assertIn("stage=db status=OK elapsed_seconds=", messages)
        self.assertEqual(read_sql.call_count, 2)
        process_frames.assert_called_once()
        self.assertEqual(process_frames.call_args.args[0], dataset)
        self.assertEqual(set(process_frames.call_args.args[1]), {"trails", "segments"})
        replace_group.assert_called_once_with(
            engine,
            processed_frames,
            primary_keys={
                "trails": ("route_id",),
                "segments": ("segment_id",),
            },
            indexes={
                "segments": (
                    ("idx_segments_route", ("route_id",)),
                )
            },
            allow_empty_tables=set(),
        )
        engine.dispose.assert_not_called()

    def test_durunubi_relation_validator_rejects_orphan_segments(self):
        frames = {
            "durunubi_trails": pd.DataFrame({"routeIdx": ["R1"]}),
            "durunubi_segments": pd.DataFrame(
                {"crsIdx": ["S1", "S2"], "routeIdx": ["R1", "R2"]}
            ),
        }

        with self.assertRaisesRegex(RuntimeError, "orphan routeIdx=1건"):
            validate_durunubi_relations(frames)


if __name__ == "__main__":
    unittest.main()
