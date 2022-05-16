import csv
import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print the values from a multivalue CSV column to stdout"
    )
    parser.add_argument("csv_path", type=Path, help="Path to the multivalue CSV")
    parser.add_argument(
        "column_name", type=str, help="The column name to print",
    )
    parser.add_argument(
        "--dialect",
        type=str,
        default="excel-tab",
        choices=csv.list_dialects(),
        help="dialect of input CSV",
    )
    parser.add_argument(
        "--value-sep", type=str, default="; ", help="The column value separator",
    )
    args = parser.parse_args()

    with open(args.csv_path, mode="r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp, dialect=args.dialect)
        for row in reader:
            values = row[args.column_name].split(args.value_sep)
            for value in values:
                if value:
                    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
