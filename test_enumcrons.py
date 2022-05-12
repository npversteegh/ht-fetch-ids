import csv
import sys
import argparse
import dataclasses

from pathlib import Path

import ht_fetch_ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show the output of enumcron parsing on a text file of enumcrons"
    )
    parser.add_argument(
        "enumcrons_path",
        type=Path,
        help="Path to the newline separated batch of enumcrons",
    )
    args = parser.parse_args()

    with open(args.enumcrons_path, mode="r", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=["enumcron"]
            + [field.name for field in dataclasses.fields(ht_fetch_ids.Enumcron)],
            dialect="excel-tab",
        )
        writer.writeheader()
        for line in fp:
            enumcron = line.strip()
            writer.writerow(
                {
                    "enumcron": enumcron,
                    **dataclasses.asdict(ht_fetch_ids.extract_enumcron(enumcron)),
                }
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
