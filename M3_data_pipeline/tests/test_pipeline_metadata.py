import sys
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

from dataset_processors import PROCESSORS  # noqa: E402
from pipeline_metadata import SourceKind, get_dataset_specs  # noqa: E402


class PipelineMetadataTest(unittest.TestCase):
    def setUp(self):
        self.specs = get_dataset_specs()
        self.tables = [
            table
            for dataset in self.specs.values()
            for table in dataset.tables
        ]

    def test_every_registered_processor_is_declared_once(self):
        declared_processors = [table.processor for table in self.tables]

        self.assertEqual(set(declared_processors), set(PROCESSORS))
        self.assertEqual(len(declared_processors), len(set(declared_processors)))

    def test_every_physical_table_has_one_logical_owner(self):
        table_names = [table.table for table in self.tables]

        self.assertEqual(len(table_names), len(set(table_names)))
        self.assertEqual(set(table_names), set(PROCESSORS))

    def test_source_contracts_are_fully_declarative(self):
        for dataset in self.specs.values():
            with self.subTest(dataset=dataset.name):
                self.assertTrue(dataset.collector_script)
                self.assertTrue(dataset.default_cron)
                for table in dataset.tables:
                    self.assertIs(table.source_kind, dataset.source_kind)
                    self.assertIs(table.source_kind, SourceKind.RAW_DATABASE)

    def test_existing_constraints_and_empty_snapshot_contract_are_preserved(self):
        bicycle = self.specs["bicycle_accident"].tables[0]
        self.assertEqual(bicycle.primary_key, ("afos_fid",))
        self.assertEqual(
            {(index.name, index.columns) for index in bicycle.indexes},
            {
                ("ix_bicycle_region", ("search_year", "si_do", "gu_gun")),
                ("ix_bicycle_spot", ("spot_cd",)),
            },
        )

        durunubi = {
            table.table: table
            for table in self.specs["durunubi"].tables
        }
        self.assertEqual(durunubi["durunubi_trails"].primary_key, ("routeIdx",))
        self.assertEqual(durunubi["durunubi_segments"].primary_key, ("crsIdx",))
        self.assertEqual(len(durunubi["durunubi_segments"].indexes), 3)

        warning = {
            table.table: table
            for table in self.specs["weather_warning"].tables
        }
        self.assertFalse(warning["weather_warning"].allow_empty)
        self.assertTrue(warning["weather_warning_status"].allow_empty)

    def test_default_datasets_have_no_final_barrier(self):
        run_last = [
            dataset.name
            for dataset in self.specs.values()
            if dataset.run_last
        ]

        self.assertEqual(run_last, [])


if __name__ == "__main__":
    unittest.main()
