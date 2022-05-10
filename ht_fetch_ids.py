import requests
import requests_cache

import csv
import argparse
import collections
import re
import sys
import io
from pathlib import Path

from typing import Iterable, Optional, Literal, Any, Iterator, Union


Item = collections.namedtuple(
    "Item", "orig fromRecord htid itemURL rightsCode lastUpdate enumcron usRightsString"
)
BriefRecord = collections.namedtuple(
    "BriefRecord", "recordnumber recordURL titles isbns issns oclcs, lccns publishDates"
)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get HathiTrust htids based on Sierra Create List exports"
    )
    parser.add_argument("export_path", type=Path, help="Path to Sierra export file")
    parser.add_argument(
        "--field-delimiter",
        type=str,
        default="\t",
        help="Sierra export field delimiter",
    )
    parser.add_argument(
        "--text-qualifier", type=str, default='"', help="Sierra export text qualifier"
    )
    parser.add_argument(
        "--repeated-field-delimiter",
        type=str,
        default=";",
        help="Sierra export repeated field delimiter",
    )
    parser.add_argument(
        "--dialect", choices=csv.list_dialects(), default="unix", help="Results format"
    )
    parser.add_argument(
        "--delay", type=int, default=0, help="Seconds of delay between HT API requests"
    )
    parser.add_argument(
        "--oclc-column",
        type=str,
        default="OCLC #",
        help="Sierra export OCLC number column name",
    )
    parser.add_argument(
        "--lccn-column",
        type=str,
        default="LC CARD #",
        help="Sierra export LCCN column name",
    )
    parser.add_argument(
        "--isn-column",
        type=str,
        default="ISBN/ISSN",
        help="Sierra export ISBN/ISSN column name",
    )
    parser.add_argument(
        "--http-cache",
        type=Path,
        default=None,
        help="Path to optional http requests cache",
    )
    args = parser.parse_args()

    with open(args.export_path, mode="r", encoding="utf-8") as fp:
        reader = read_sierra_export(
            fp,
            field_delimiter=args.field_delimiter,
            text_qualifier=args.text_qualifier,
            repeated_field_delimiter=args.repeated_field_delimiter,
        )

        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=reader.fieldnames
            + ["ht-recordURLs", "enumcrons", "group-counts", "htids"],
            dialect="excel-tab",
        )
        writer.writeheader()

        with (
            requests_cache.CachedSession(args.http_cache, backend="sqlite")
            if args.http_cache
            else requests.Session()
        ) as session:
            for row in reader:
                oclc = row.get(args.oclc_column)
                lccn = row.get(args.lccn_column)
                isns = row.get(args.isn_column)
                result = search_ht(
                    oclc=oclc[0] if oclc else None,
                    lccn=lccn[0] if lccn else None,
                    isns=isns,
                    session=session,
                )
                if result:
                    items = [Item(**item_args) for item_args in result["items"]]
                    row["ht-recordURLs"] = [
                        record["recordURL"] for record in result["records"].values()
                    ]
                    row["enumcrons"] = list(
                        {item.enumcron for item in items if item.enumcron is not False}
                    )
                    groups = group_items_by_origin(items)
                    row["group-counts"] = [
                        str(len(group_items)) for group_items in groups.values()
                    ]
                    row["htids"] = [item.htid for item in pick_group(groups)]
                writer.writerow({key: "; ".join(values) for key, values in row.items()})


def parse_results(result: dict[str, Any]) -> list[tuple[BriefRecord, list[Item]]]:
    records_and_items = []
    for recordnumber, record_data in result["records"].items():
        record = BriefRecord(recordnumber=recordnumber, **record_data)
        items = [Item(**item_data) for item_data in result["items"] if item_data["fromRecord"] == recordnumber]
        records_and_items.append((record, items))
    assert sum(len(items) for _, items in records_and_items) == len(result["items"])
    return records_and_items


def pick_record_and_items(records_and_items: list[tuple[BriefRecord, list[Item]]]) -> tuple[BriefRecord, list[Item]]:
    return max(records_and_items, key=lambda r_and_i: len(r_and_i[1]))


def pick_group(groups: dict[tuple[str, str], list[Item]]) -> list[Item]:
    if not groups:
        return []
    return max((group for group in groups.values()), key=score_group)


def score_group(group: list[Item]) -> float:
    enumcrons = {item.enumcron for item in group}
    missing_enumcron_penalty = 1 if False in enumcrons and len(enumcrons) > 1 else 0
    return float(f"{len(group)}.{most_recent_update(group)}") - missing_enumcron_penalty


def most_recent_update(items: Iterable[Item]) -> int:
    return max(int(item.lastUpdate) for item in items)


def group_items_by_origin(items: Iterable[Item]) -> dict[tuple[str, str], list[Item]]:
    groups = collections.defaultdict(list)
    for item in items:
        groups[(item.orig, item.fromRecord)].append(item)
    return {origin: dedupe_enumcron(group) for origin, group in groups.items()}


def dedupe_enumcron(items: Iterable[Item]) -> list[Item]:
    volumes = dict()
    for item in items:
        normalcron = normalize_enumcron(item.enumcron)
        if normalcron != item.enumcron:
            print(
                f"Normalized enumcron {item.enumcron!r} --> {normalcron!r}", file=sys.stderr
            )
        if normalcron in volumes:
            if int(item.lastUpdate) < int(volumes[normalcron].lastUpdate):
                continue
        volumes[normalcron] = item
    if False in volumes and len(volumes) > 1:
        print(
            f"Multivolume set with missing enumcrons, record {volumes[False].fromRecord}",
            file=sys.stderr,
        )
    return list(volumes.values())


def normalize_enumcron(enumcron: Union[str, bool]) -> Union[str, bool]:
    if enumcron is False:
        return False
    return zap_copy_number(enumcron)


def zap_copy_number(enumcron: str) -> Union[str, bool]:
    zapped = re.sub(r"(^|\W)c(opy)?[\. ]?\d+", "", enumcron).strip()
    return zapped if zapped.strip("[](){}<>, -") else False


assert zap_copy_number("c.1 v.2") == "v.2"
assert zap_copy_number("copy 2") == False
assert zap_copy_number("[copy 2]") == False
assert zap_copy_number("c.2") == False
assert zap_copy_number("Dec 1999") == "Dec 1999"

def is_copy(enumcron: str) -> bool:
    normal = "".join(enumcron.split(" ")).strip("[](){}<>")
    return bool(
        any(re.match(regex, normal) for regex in [r"^copy\d+$", r"^c.\d+$", r"^---$"])
    )


assert is_copy("copy 2")
assert is_copy("[copy 2]")
assert is_copy("c.1")
assert is_copy("c. 1")
assert is_copy("---")
assert not is_copy("c.1 v.1")


def search_ht(
    oclc: Optional[str], lccn: Optional[str], isns: list[str], session: requests.Session
) -> Optional[dict[str, Any]]:
    search_order = []

    if oclc:
        search_order.append((oclc, "oclc"))
    lccn_num = search_for_lccn(lccn) if lccn else None
    if lccn_num:
        search_order.append((lccn_num, "lccn"))
    for value in isns:
        try:
            search_order.append(search_for_isn(value))
            break
        except ValueError:
            continue

    for id_, id_type in search_order:
        result = query_ht_bib_api(
            id_=id_, id_type=id_type, return_type="brief", session=session
        )
        if result["items"]:
            return result
    return None


def search_for_isn(value: str) -> tuple[str, str]:
    if match := re.search(r"^[0-9]{4}-?[0-9]{3}[0-9xX]$", value):
        return match.group(), "issn"
    if match := re.search(r"[0-9]{12}[0-9xX]", value):
        return match.group(), "isbn"
    if match := re.search(r"[0-9]{9}[0-9xX]", value):
        return match.group(), "isbn"
    raise ValueError("no valid ISBN/ISSN found")


assert search_for_isn("1234-456x") == ("1234-456x", "issn")
assert search_for_isn("123456789x") == ("123456789x", "isbn")
assert search_for_isn("1234567890 extra") == ("1234567890", "isbn")
assert search_for_isn("before 123456789012x") == ("123456789012x", "isbn")


def search_for_lccn(value: str) -> str:
    if match := re.search(r"[0-9]{8}", value):
        return match.group()
    raise ValueError("no valid LCCN found")


def query_ht_bib_api(
    id_: str,
    id_type: Literal["oclc", "lccn", "issn", "isbn", "htid", "recordnumber"],
    return_type: Literal["brief", "full"],
    session: requests.Session,
) -> dict[str, Any]:
    response = session.get(
        f"https://catalog.hathitrust.org/api/volumes/{return_type}/{id_type}/{id_}.json"
    )
    response.raise_for_status()
    return response.json()


def read_sierra_export(
    fp: io.StringIO,
    field_delimiter: str = "\t",
    text_qualifier: str = '"',
    repeated_field_delimiter: str = ";",
) -> "SierraExportReader":
    sierra_export_dialect_name = "sierra"
    csv.register_dialect(
        sierra_export_dialect_name, delimiter=field_delimiter, quoting=csv.QUOTE_NONE
    )
    reader = csv.DictReader(fp, dialect=sierra_export_dialect_name)
    return SierraExportReader(
        reader,
        text_qualifier=text_qualifier,
        repeated_field_delimter=repeated_field_delimiter,
    )


def _split_repeated_values(
    values: str, text_qualifier: str, repeated_field_delimiter: str
) -> list[str]:
    values_list = values.split(
        text_qualifier + repeated_field_delimiter + text_qualifier
    )
    values_list[0] = values_list[0].lstrip(text_qualifier)
    values_list[-1] = values_list[-1].rstrip(text_qualifier)
    return values_list if values_list[0] else []


assert _split_repeated_values('"1"', '"', ";") == ["1"]
assert _split_repeated_values('"1";"2";"3"', '"', ";") == ["1", "2", "3"]
assert _split_repeated_values("", '"', ";") == []


class SierraExportReader(collections.abc.Iterable):
    fieldnames: list[str]
    _dict_reader: csv.DictReader

    def __init__(
        self,
        dict_reader: csv.DictReader,
        text_qualifier: str,
        repeated_field_delimter: str,
    ) -> None:
        self._dict_reader = dict_reader
        self.text_qualifier = text_qualifier
        self.repeated_field_delimiter = repeated_field_delimter
        self.fieldnames = dict_reader.fieldnames

    def __iter__(self) -> Iterator:
        for row in self._dict_reader:
            yield {
                key: _split_repeated_values(
                    values,
                    text_qualifier=self.text_qualifier,
                    repeated_field_delimiter=self.repeated_field_delimiter,
                )
                for key, values in row.items()
            }


if __name__ == "__main__":
    main()
