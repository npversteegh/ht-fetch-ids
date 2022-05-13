import requests
import requests_cache

import csv
import argparse
import collections
import dataclasses
import re
import sys
import io
import datetime
from pathlib import Path

from typing import Iterable, Optional, Literal, Any, Iterator, Union, Callable


Item = collections.namedtuple(
    "Item", "orig fromRecord htid itemURL rightsCode lastUpdate enumcron usRightsString"
)
BriefRecord = collections.namedtuple(
    "BriefRecord", "recordnumber recordURL titles isbns issns oclcs, lccns publishDates"
)


def main() -> int:
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
        "--volume-column",
        type=str,
        default="VOLUME",
        help="Sierra export volume designators column name",
    )
    parser.add_argument(
        "--match-volumes",
        action="store_true",
        help="Try to match volume holdings against HT enumcrons",
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
            + ["ht-recordURLs", "enumcrons", "group-counts"]
            + (["volume-match-pct"] if args.match_volumes else [])
            + ["htids"],
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
                vols = row.get(args.volume_column)
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
                    enumcrons = {
                        item.enumcron for item in items if item.enumcron is not False
                    }
                    row["enumcrons"] = list(enumcrons)
                    groups = group_items_by_origin(items)
                    row["group-counts"] = [
                        str(len(group_items)) for group_items in groups.values()
                    ]
                    if args.match_volumes and enumcrons and row[args.volume_column]:
                        selected_items = pick_volumes(
                            items, row[args.volume_column], match_volume_range
                        )
                        row[
                            "volume-match-pct"
                        ] = [f"{(len(selected_items) / len(set(row[args.volume_column]))) * 100:.1f}"]
                    else:
                        selected_items = pick_group(groups)
                    row["htids"] = [item.htid for item in selected_items]
                writer.writerow({key: "; ".join(values) for key, values in row.items()})
    return 0


def pick_volumes(
    items: list[Item],
    holdings: list[str],
    matcher: Callable[["Enumcron", "Enumcron"], bool],
) -> list[Item]:
    if not holdings:
        raise ValueError("no holdings provided")
    if not items:
        raise ValueError("no items to match")
    held_enumcrons = [extract_enumcron(holding) for holding in set(holdings)]
    ht_enumcrons = dict()
    for item in items:
        ht_enumcron = extract_enumcron(item.enumcron)
        if ht_enumcron in ht_enumcrons:
            if int(item.lastUpdate) < int(ht_enumcrons[ht_enumcron].lastUpdate):
                continue
        ht_enumcrons[ht_enumcron] = item
    matches = set()
    for held_enumcron in held_enumcrons:
        for ht_enumcron, item in ht_enumcrons.items():
            if matcher(held_enumcron, ht_enumcron):
                matches.add(ht_enumcron)
    if len(matches) < len(held_enumcrons):
        print(
            f"Missed {len(held_enumcrons) - len(matches)} volumes of {len(held_enumcrons)}",
            file=sys.stderr,
        )
    return [ht_enumcrons[match] for match in matches]


def match_volume_exact(first: "Enumcron", second: "Enumcron") -> bool:
    return first == second


def match_volume_range(first: "Enumcron", second: "Enumcron") -> bool:
    if first.volumespan and second.volumespan:
        return match_spans(first.volumespan, second.volumespan)
    if first.numberspan and second.numberspan:
        return match_spans(first.numberspan, second.numberspan)
    if first.datespan and second.datespan:
        return match_spans(first.datespan, second.datespan)
    if first == Enumcron() and second == Enumcron():
        return True
    return False


def match_spans(first: tuple[int, int], second: tuple[int, int]) -> bool:
    return bool(first[0] <= second[1] and first[1] >= second[0])


@dataclasses.dataclass(frozen=True)
class Enumcron:
    volumespan: Optional[tuple[int, int]] = None
    partspan: Optional[tuple[int, int]] = None
    numberspan: Optional[tuple[int, int]] = None
    seriesspan: Optional[tuple[int, int]] = dataclasses.field(default=None, compare=False)
    datespan: Optional[tuple[int, int]] = None
    copyspan: Optional[tuple[int, int]] = dataclasses.field(default=None, compare=False)
    is_index: bool = False
    is_supplement: bool = False
    remainder: Optional[str] = dataclasses.field(default=None, compare=False)


def extract_enumcron(enumcron: str) -> Enumcron:
    if not enumcron:
        return Enumcron()
    if enumcron.isdigit() and int(enumcron) < 1000:
        return Enumcron(volumespan=(int(enumcron), int(enumcron)))

    remainder = translate_to_english(enumcron)
    remainder, volumespan = extract_volumespan(remainder)
    remainder, partspan = extract_partspan(remainder)
    remainder, numberspan = extract_numberspan(remainder)
    remainder, seriesspan = extract_seriesspan(remainder)
    remainder, datespan = extract_datespan(remainder)
    remainder, copyspan = extract_copyspan(remainder)
    remainder, is_index = extract_is_index(remainder)
    remainder, is_supplement = extract_is_supplement(remainder)

    remainder = remainder.strip(" ,()[]")
    remainder = remainder or None

    return Enumcron(
        volumespan=volumespan,
        partspan=partspan,
        numberspan=numberspan,
        seriesspan=seriesspan,
        datespan=datespan,
        copyspan=copyspan,
        is_index=is_index,
        is_supplement=is_supplement,
        remainder=remainder,
    )


def extract_volumespan(enumcron: str) -> tuple[str, Optional[tuple[int, int]]]:
    return extract_simple_span(r"v(?:ol)?\.? ?(\d+)(?:-(\d+))?,?", enumcron)


def extract_numberspan(enumcron: str) -> tuple[str, Optional[tuple[int, int]]]:
    return extract_simple_span(r"(?:^|[^a-z])n(?:o)?\.? ?(\d+)(?:-(\d+))?,?", enumcron)


def extract_partspan(enumcron: str) -> tuple[str, Optional[tuple[int, int]]]:
    return extract_simple_span(r"p(?:t)?\.? ?(\d+)(?:-(\d+))?,?", enumcron)


def extract_seriesspan(enumcron: str) -> tuple[str, Optional[tuple[int, int]]]:
    remainder, match = search_and_remove(
        r"(nser|n\.s\.)|(?:(?:^|[^a-z])s(?:er)?\.? ?(\d+)(?:-(\d+))?),?", enumcron
    )
    if not match:
        return remainder, None
    no_series, start, end = match.groups()
    if no_series:
        return remainder, None
    return remainder, (int(start), int(end) if end else int(start))


def extract_copyspan(enumcron: str) -> tuple[str, Optional[tuple[int, int]]]:
    return extract_simple_span(
        r"(?:^|[^a-z])c(?:(?:opy)|(?:p))?\.? ?(\d+)(?:-(\d+))?,?", enumcron
    )


def extract_datespan(
    enumcron: str,
) -> tuple[str, Optional[tuple[datetime.date, datetime.date]]]:
    remainder, match = search_and_remove(
        r"(?:(?:yr\. ?)?(\d{4})(?:[-/ ](\d+))?)", enumcron
    )
    if not match:
        return remainder, None
    start, end = match.groups()
    if end and len(end) == 2:
        end = start[:2] + end
    return (
        remainder,
        (
            datetime.date(year=int(start), month=1, day=1),
            datetime.date(year=int(end) if end else int(start), month=12, day=31),
        ),
    )


def extract_simple_span(
    pattern: str, enumcron: str
) -> tuple[str, Optional[tuple[int, int]]]:
    remainder, match = search_and_remove(pattern, enumcron)
    if not match:
        return remainder, None
    start, end = match.groups()
    return remainder, (int(start), int(end) if end else int(start))


def extract_is_index(enumcron: str) -> tuple[str, bool]:
    remainder, match = search_and_remove(r"(index:?)", enumcron)
    return remainder, bool(match)


def extract_is_supplement(enumcron: str) -> tuple[str, bool]:
    remainder, match = search_and_remove(r"(suppl\.?)", enumcron)
    return remainder, bool(match)


def search_and_remove(pattern: str, s: str) -> tuple[str, Optional[re.Match]]:
    match = re.search(pattern, s.casefold())
    if match is None:
        return s, None
    return s[: match.start()] + " " + s[match.end() :], match


def translate_to_english(enumcron: str) -> str:
    translations = {
        "jahrg": "v",
        "Jahrg": "v",
        "bd": "pt",
    }
    translated = enumcron
    for foreign, english in translations.items():
        translated = translated.replace(foreign, english)
    return translated


assert extract_enumcron("v.1") == Enumcron(volumespan=(1, 1))
assert extract_enumcron("v. 1") == Enumcron(volumespan=(1, 1))
assert extract_enumcron("V. 123") == Enumcron(volumespan=(123, 123))
assert extract_enumcron("NO. 1") == Enumcron(numberspan=(1, 1))
assert extract_enumcron("pt. 1") == Enumcron(partspan=(1, 1))
assert extract_enumcron("1900") == Enumcron(
    datespan=(datetime.date(1900, 1, 1), datetime.date(1900, 12, 31))
)
assert extract_enumcron("1926-27") == Enumcron(
    datespan=(datetime.date(1926, 1, 1), datetime.date(1927, 12, 31))
)
assert extract_enumcron("2002-2003") == Enumcron(
    datespan=(datetime.date(2002, 1, 1), datetime.date(2003, 12, 31))
)
assert extract_enumcron("v.1, 1900") == Enumcron(
    volumespan=(1, 1), datespan=(datetime.date(1900, 1, 1), datetime.date(1900, 12, 31))
)


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
                f"Normalized enumcron {item.enumcron!r} --> {normalcron!r}",
                file=sys.stderr,
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


def search_ht(
    oclc: Optional[str], lccn: Optional[str], isns: list[str], session: requests.Session
) -> Optional[dict[str, Any]]:
    search_order = []

    if oclc and oclc.strip().isdigit():
        search_order.append((oclc.strip(), "oclc"))

    try:
        lccn_num = search_for_lccn(lccn) if lccn else None
    except ValueError:
        lccn_num = None
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
    raise SystemExit(main())
