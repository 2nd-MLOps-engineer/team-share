"""Declarative metadata for the M3 data pipeline.

This module intentionally contains no collection, processing, DQ, or database
loading code.  A future collector discovery/schema profiling implementation only
needs to provide the same ``MetadataProvider`` interface.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Protocol


class SourceKind(str, Enum):
    RAW_DATABASE = "raw_database"


@dataclass(frozen=True)
class IndexSpec:
    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class TableSpec:
    """Metadata for one physical RAW/PROCESSED table pair."""

    table: str
    processor: str
    source_kind: SourceKind
    allow_empty: bool = False
    primary_key: tuple[str, ...] = ()
    indexes: tuple[IndexSpec, ...] = ()

@dataclass(frozen=True)
class DatasetSpec:
    """Metadata for one logical collection/orchestration unit.

    ``run_last`` datasets form a final execution barrier for ``--run-all-once``.
    The final barrier starts only after every regular dataset succeeds.  This
    keeps expensive collectors out of failed validation runs and leaves a clear
    phase boundary for a future bounded executor.
    """

    name: str
    collector_script: str
    default_cron: str
    tables: tuple[TableSpec, ...]
    validator: str | None = None
    default_attempts: int = 3
    run_order: int = 100
    run_last: bool = False

    def __post_init__(self) -> None:
        if not self.tables:
            raise ValueError(f"{self.name}: physical table이 없습니다.")
        source_kinds = {table.source_kind for table in self.tables}
        if len(source_kinds) != 1:
            raise ValueError(
                f"{self.name}: 하나의 dataset에서 source kind를 혼합할 수 없습니다."
            )
        table_names = [table.table for table in self.tables]
        if len(table_names) != len(set(table_names)):
            raise ValueError(f"{self.name}: physical table 이름이 중복됩니다.")

    @property
    def source_kind(self) -> SourceKind:
        return self.tables[0].source_kind


class MetadataProvider(Protocol):
    """Extension point for static metadata or future discovery/profiling."""

    def datasets(self) -> Mapping[str, DatasetSpec]:
        ...


class StaticMetadataProvider:
    def __init__(self, specs: tuple[DatasetSpec, ...]):
        by_name = {spec.name: spec for spec in specs}
        if len(by_name) != len(specs):
            raise ValueError("logical dataset 이름이 중복됩니다.")
        run_last = [spec.name for spec in specs if spec.run_last]
        if len(run_last) > 1:
            raise ValueError(
                "run_last logical dataset은 하나만 등록할 수 있습니다: "
                f"{run_last}"
            )
        self._datasets = MappingProxyType(by_name)

    def datasets(self) -> Mapping[str, DatasetSpec]:
        return self._datasets


STATIC_DATASETS = (
    DatasetSpec(
        name="weather",
        collector_script="collector/weather.py",
        default_cron="10,40 * * * *",
        tables=(
            TableSpec(
                table="weather_ultra_ncst",
                processor="weather_ultra_ncst",
                source_kind=SourceKind.RAW_DATABASE,
            ),
            TableSpec(
                table="weather_ultra_fcst",
                processor="weather_ultra_fcst",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=10,
    ),
    DatasetSpec(
        name="air_quality",
        collector_script="collector/air_quality.py",
        default_cron="15 * * * *",
        tables=(
            TableSpec(
                table="air_quality",
                processor="air_quality",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=20,
    ),
    DatasetSpec(
        name="weather_warning",
        collector_script="collector/weather_warning.py",
        default_cron="*/10 * * * *",
        tables=(
            TableSpec(
                table="weather_warning",
                processor="weather_warning",
                source_kind=SourceKind.RAW_DATABASE,
            ),
            TableSpec(
                table="weather_warning_status",
                processor="weather_warning_status",
                source_kind=SourceKind.RAW_DATABASE,
                allow_empty=True,
            ),
        ),
        run_order=30,
    ),
    DatasetSpec(
        name="durunubi",
        collector_script="collector/durunubi_api.py",
        default_cron="0 4 * * 1",
        tables=(
            TableSpec(
                table="durunubi_trails",
                processor="durunubi_trails",
                source_kind=SourceKind.RAW_DATABASE,
                primary_key=("routeIdx",),
            ),
            TableSpec(
                table="durunubi_segments",
                processor="durunubi_segments",
                source_kind=SourceKind.RAW_DATABASE,
                primary_key=("crsIdx",),
                indexes=(
                    IndexSpec("idx_durunubi_segments_routeidx", ("routeIdx",)),
                    IndexSpec("idx_durunubi_segments_sigun", ("sigun",)),
                    IndexSpec("idx_durunubi_segments_level", ("crsLevel",)),
                ),
            ),
        ),
        validator="durunubi_relations",
        run_order=40,
    ),
    DatasetSpec(
        name="facility",
        collector_script="collector/facility_api_v2.py",
        default_cron="0 4 1 * *",
        tables=(
            TableSpec(
                table="facility",
                processor="facility",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=50,
    ),
    DatasetSpec(
        name="public_open_facility",
        collector_script="collector/open_facil_api.py",
        default_cron="0 5 1 * *",
        tables=(
            TableSpec(
                table="public_open_facility",
                processor="public_open_facility",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=60,
    ),
    DatasetSpec(
        name="aed",
        collector_script="collector/AED_api.py",
        default_cron="0 3 2 * *",
        tables=(
            TableSpec(
                table="aed",
                processor="aed",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=70,
    ),
    DatasetSpec(
        name="bicycle_accident",
        collector_script="collector/bicycle_accident_api.py",
        default_cron="0 2 3 * *",
        tables=(
            TableSpec(
                table="koroad_bicycle_accident_hotspots",
                processor="koroad_bicycle_accident_hotspots",
                source_kind=SourceKind.RAW_DATABASE,
                primary_key=("afos_fid",),
                indexes=(
                    IndexSpec(
                        "ix_bicycle_region",
                        ("search_year", "si_do", "gu_gun"),
                    ),
                    IndexSpec("ix_bicycle_spot", ("spot_cd",)),
                ),
            ),
        ),
        default_attempts=2,
        run_order=80,
    ),
    DatasetSpec(
        name="culture_bigdata",
        collector_script="culture_bigdata_selenium.py",
        default_cron="0 7 1 * *",
        tables=tuple(
            TableSpec(
                table=table,
                processor=table,
                source_kind=SourceKind.RAW_DATABASE,
            )
            for table in (
                "culture_public_sports_facilities",
                "culture_public_sports_facility_programs",
                "culture_sports_facility_nearby_public_transport",
                "culture_national_sports_facility_status",
                "culture_location_fitness_measurement_prescriptions",
                "culture_open_school_sports_facilities",
                "culture_sports_facility_safety_inspections",
                "culture_fitness_measurement_prescriptions",
            )
        ),
        default_attempts=2,
        run_order=90,
    ),
    DatasetSpec(
        name="bus_stop",
        collector_script="collector/busstop_api.py",
        default_cron="0 3 28 * *",
        tables=(
            TableSpec(
                table="bus_stop",
                processor="bus_stop",
                source_kind=SourceKind.RAW_DATABASE,
            ),
        ),
        run_order=1000,
        run_last=True,
    ),
)


DEFAULT_METADATA_PROVIDER: MetadataProvider = StaticMetadataProvider(STATIC_DATASETS)


def get_dataset_specs(
    provider: MetadataProvider = DEFAULT_METADATA_PROVIDER,
) -> Mapping[str, DatasetSpec]:
    return provider.datasets()
