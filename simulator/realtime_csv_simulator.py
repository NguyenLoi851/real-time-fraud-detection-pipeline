#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate real-time transaction events by reading a CSV row-by-row "
            "and writing each event to an output CSV with a delay."
        )
    )
    parser.add_argument(
        "--input",
        default="data/transaction_log.csv",
        help="Path to the source CSV file.",
    )
    parser.add_argument(
        "--output",
        default="data/realtime_transactions.csv",
        help="Path to the output CSV file.",
    )
    parser.add_argument(
        "--interval-min",
        type=float,
        default=0.5,
        help="Minimum random delay in seconds between events.",
    )
    parser.add_argument(
        "--interval-max",
        type=float,
        default=2.0,
        help="Maximum random delay in seconds (used with --random-interval).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum number of events to emit (0 means no limit).",
    )
    parser.add_argument(
        "--include-labels",
        action="store_true",
        help="Include isFraud/isFlaggedFraud columns in output. Disabled by default.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists. Default behavior appends new events.",
    )
    parser.add_argument(
        "--log-row-details",
        action="store_true",
        help="Print key row values for each emitted event.",
    )
    return parser.parse_args()


def input_fieldnames(input_path: Path) -> list[str]:
    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"Input CSV has no header: {input_path}")
        return reader.fieldnames


def output_fields(fields: Iterable[str], include_labels: bool) -> list[str]:
    labels = {"isFraud", "isFlaggedFraud"}
    selected = [field for field in fields if include_labels or field not in labels]
    return selected + ["event_emitted_at_utc"]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def format_row_log(row: dict[str, str], include_labels: bool) -> str:
    parts = [
        f"step={row.get('step', '')}",
        f"type={row.get('type', '')}",
        f"amount={row.get('amount', '')}",
        f"nameOrig={row.get('nameOrig', '')}",
        f"nameDest={row.get('nameDest', '')}",
    ]
    if include_labels:
        parts.append(f"isFraud={row.get('isFraud', '')}")
        parts.append(f"isFlaggedFraud={row.get('isFlaggedFraud', '')}")
    return ", ".join(parts)


def simulate_stream(
    input_path: Path,
    output_path: Path,
    interval_min: float,
    interval_max: float,
    max_events: int,
    include_labels: bool,
    overwrite: bool,
    log_row_details: bool,
) -> int:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if interval_min < 0 or interval_max < 0:
        raise ValueError("--interval-min and --interval-max must be >= 0")

    if interval_min > interval_max:
        raise ValueError("--interval-min must be <= --interval-max")

    if max_events < 0:
        raise ValueError("--max-events must be >= 0")

    source_fields = input_fieldnames(input_path)
    target_fields = output_fields(source_fields, include_labels)

    ensure_parent(output_path)
    write_header = overwrite or (not output_path.exists() or output_path.stat().st_size == 0)
    mode = "w" if overwrite else "a"

    emitted = 0

    with input_path.open("r", newline="", encoding="utf-8") as source, output_path.open(
        mode, newline="", encoding="utf-8"
    ) as sink:
        reader = csv.DictReader(source)
        writer = csv.DictWriter(sink, fieldnames=target_fields)

        if write_header:
            writer.writeheader()

        for row in reader:
            if max_events and emitted >= max_events:
                break

            output_row = {
                key: value
                for key, value in row.items()
                if include_labels or key not in {"isFraud", "isFlaggedFraud"}
            }
            output_row["event_emitted_at_utc"] = datetime.now(timezone.utc).isoformat()
            writer.writerow(output_row)
            sink.flush()

            emitted += 1
            print(f"Emitted event {emitted}", flush=True)
            if log_row_details:
                print(f"Event details: {format_row_log(row, include_labels)}", flush=True)

            sleep_seconds = random.uniform(interval_min, interval_max)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    return emitted


def main() -> None:
    args = parse_args()

    emitted = simulate_stream(
        input_path=Path(args.input),
        output_path=Path(args.output),
        interval_min=args.interval_min,
        interval_max=args.interval_max,
        max_events=args.max_events,
        include_labels=args.include_labels,
        overwrite=args.overwrite,
        log_row_details=args.log_row_details,
    )

    print(
        f"Done. Emitted {emitted} events to {args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
