from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Iterable, Mapping


class CheckpointError(RuntimeError):
    """Checkpoint contents do not match the collection being resumed."""


def unit_fingerprint(unit_keys: Iterable[str]) -> str:
    payload = "\n".join(str(key) for key in unit_keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def atomic_write_json(path: Path, payload: Mapping) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(payload, file, ensure_ascii=False, separators=(",", ":"), default=str)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp_path, path)


def rewrite_checkpoint(
    path: Path,
    meta: Mapping,
    entries: Mapping[str, Mapping],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp.jsonl")
    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(dict(meta), ensure_ascii=False) + "\n")
        for entry in entries.values():
            file.write(
                json.dumps(
                    dict(entry),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            )
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp_path, path)


def load_checkpoint(path: Path) -> tuple[dict | None, dict[str, dict]]:
    if not path.exists():
        return None, {}

    meta = None
    entries: dict[str, dict] = {}
    invalid_line_found = False
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_line_found = True
                continue
            if record.get("type") == "meta":
                meta = record
                continue
            if record.get("type") != "unit":
                continue
            key = record.get("key")
            if not isinstance(key, str) or not key:
                raise CheckpointError(
                    f"체크포인트 {line_number}행의 unit key가 올바르지 않습니다: {path}"
                )
            entries[key] = record

    if meta is None:
        raise CheckpointError(f"체크포인트 메타데이터가 없습니다: {path}")
    if invalid_line_found:
        rewrite_checkpoint(path, meta, entries)
    return meta, entries


def validate_meta(
    actual: Mapping,
    expected: Mapping,
    fields: Iterable[str],
    path: Path,
) -> None:
    mismatches = [field for field in fields if actual.get(field) != expected.get(field)]
    if mismatches:
        raise CheckpointError(
            "기존 체크포인트가 현재 수집 범위와 다릅니다: "
            f"{path} (fields={','.join(mismatches)})"
        )


def validate_entries(
    entries: Mapping[str, Mapping],
    valid_keys: Iterable[str],
    path: Path,
    *,
    statuses: Iterable[str] = ("success",),
) -> None:
    allowed_keys = set(valid_keys)
    allowed_statuses = set(statuses)
    invalid_keys = set(entries).difference(allowed_keys)
    if invalid_keys:
        raise CheckpointError(
            "체크포인트에 현재 수집 범위에 없는 key가 있습니다: "
            f"{path} ({','.join(sorted(invalid_keys))})"
        )
    for key, entry in entries.items():
        if entry.get("key") != key or entry.get("status") not in allowed_statuses:
            raise CheckpointError(f"체크포인트 unit 상태가 올바르지 않습니다: {path} ({key})")
        if not isinstance(entry.get("records"), list):
            raise CheckpointError(f"체크포인트 records가 list가 아닙니다: {path} ({key})")


def append_checkpoint_entry(path: Path, entry: Mapping) -> None:
    if not path.exists():
        raise CheckpointError(f"체크포인트 메타데이터 파일이 없습니다: {path}")
    with path.open("a", encoding="utf-8", newline="\n") as file:
        file.write(
            json.dumps(
                dict(entry),
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            )
            + "\n"
        )
        file.flush()
        os.fsync(file.fileno())


def remove_checkpoint_after_success(path: Path) -> None:
    if path.exists():
        path.unlink()
