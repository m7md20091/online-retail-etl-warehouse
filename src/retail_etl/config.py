from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path
    source_file: Path
    warehouse_file: Path
    archive_dir: Path
    quality_dir: Path
    export_dir: Path
    log_dir: Path
    maximum_rejected_percentage: float
    maximum_duplicate_percentage: float
    minimum_valid_rows: int

    @classmethod
    def load(cls, config_path: str | Path = "config/pipeline.json") -> "Settings":
        config_path = Path(config_path).resolve()
        root = config_path.parent.parent
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        limits = raw["quality_thresholds"]

        def absolute(value: str) -> Path:
            candidate = Path(value)
            return candidate if candidate.is_absolute() else root / candidate

        return cls(
            root=root,
            source_file=absolute(raw["source_file"]),
            warehouse_file=absolute(raw["warehouse_file"]),
            archive_dir=absolute(raw["archive_dir"]),
            quality_dir=absolute(raw["quality_dir"]),
            export_dir=absolute(raw["export_dir"]),
            log_dir=absolute(raw["log_dir"]),
            maximum_rejected_percentage=float(limits["maximum_rejected_percentage"]),
            maximum_duplicate_percentage=float(limits["maximum_duplicate_percentage"]),
            minimum_valid_rows=int(limits["minimum_valid_rows"]),
        )

    def create_directories(self) -> None:
        for path in (
            self.warehouse_file.parent,
            self.archive_dir,
            self.quality_dir,
            self.export_dir,
            self.log_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

