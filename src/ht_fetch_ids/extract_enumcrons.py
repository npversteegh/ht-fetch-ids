import csv
import sys
import argparse
import dataclasses
import io

from pathlib import Path

from ht_fetch_ids import ht_fetch_ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show the output of enumcron parsing on a text file of enumcrons"
    )
    parser.add_argument(
        "enumcrons_path",
        type=Path,
        nargs="?",
        help="Path to the newline separated batch of enumcrons",
    )
    args = parser.parse_args()

    if args.enumcrons_path:
        with open(args.enumcrons_path, mode="r", encoding="utf-8") as fp:
            write_extracted_enumcrons(fp)
    else:
        write_extracted_enumcrons(sys.stdin)

    return 0


def write_extracted_enumcrons(fp: io.StringIO) -> None:
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


if __name__ == "__main__":
    raise SystemExit(main())
