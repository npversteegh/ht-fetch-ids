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
import functools
import itertools
import math
from pathlib import Path

from typing import (
    Iterable,
    Optional,
    Literal,
    Any,
    Iterator,
    Union,
    Callable,
    Hashable,
    Sequence,
)


Item = collections.namedtuple(
    "Item", "orig fromRecord htid itemURL rightsCode lastUpdate enumcron usRightsString"
)


BriefRecord = collections.namedtuple(
    "BriefRecord", "recordnumber recordURL titles isbns issns oclcs, lccns publishDates"
)


@dataclasses.dataclass(frozen=True)
class Enumcron:
    seriesspan: Optional[tuple[int, int]] = dataclasses.field(
        default=None, compare=False
    )
    volumespan: Optional[tuple[int, int]] = None
    numberspan: Optional[tuple[int, int]] = None
    partspan: Optional[tuple[int, int]] = None
    datespan: Optional[tuple[int, int]] = None
    copyspan: Optional[tuple[int, int]] = dataclasses.field(default=None, compare=False)
    is_index: bool = False
    is_supplement: bool = False
    remainder: Optional[str] = dataclasses.field(default=None, compare=False)
    raw: Optional[str] = dataclasses.field(default=None, compare=False)


MatchStrategy = Callable[[set[Enumcron], set[Enumcron]], dict[Enumcron, set[Enumcron]]]
MATCH_STRATEGIES: dict[str, MatchStrategy] = dict()
SPAN_ORDER = [
    "seriesspan",
    "volumespan",
    "numberspan",
    "partspan",
    "datespan",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Get HathiTrust htids based on Sierra Create List exports",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        "--output-dialect",
        choices=csv.list_dialects(),
        default="excel-tab",
        help="Output CSV format",
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
        "--vol-matcher",
        choices=list(MATCH_STRATEGIES),
        default=None,
        help="Strategy for volume matching against HT enumcrons",
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
            + (["volume-match-pct", "enumcron-matches"] if args.vol_matcher else [])
            + ["htids"],
            dialect=args.output_dialect,
        )
        writer.writeheader()

        with (
            requests_cache.CachedSession(args.http_cache, backend="sqlite")
            if args.http_cache
            else requests.Session()
        ) as session:
            for row in reader:
                oclcs = row.get(args.oclc_column)
                lccns = row.get(args.lccn_column)
                isns = row.get(args.isn_column)
                vols = row.get(args.volume_column)
                result = search_ht(
                    oclc=oclcs[0] if oclcs else None,
                    lccn=lccns[0] if lccns else None,
                    isns=isns,
                    session=session,
                )
                if result:
                    records = [
                        BriefRecord(recordnumber=recordnumber, **record_args)
                        for recordnumber, record_args in result["records"].items()
                    ]
                    items = [Item(**item_args) for item_args in result["items"]]
                    row["ht-recordURLs"] = [record.recordURL for record in records]
                    enumcrons = {
                        item.enumcron for item in items if item.enumcron is not False
                    }
                    row["enumcrons"] = list(enumcrons)
                    groups = group_items_by_origin(items)
                    row["group-counts"] = [
                        str(len(group_items)) for group_items in groups.values()
                    ]
                    if args.vol_matcher and enumcrons and row[args.volume_column]:
                        selected_items, enumcron_matches = pick_volumes(
                            items,
                            row[args.volume_column],
                            MATCH_STRATEGIES[args.vol_matcher],
                        )
                        row["volume-match-pct"] = [
                            f"{(len(selected_items) / len(set(extract_enumcron(v) for v in row[args.volume_column]))) * 100:.1f}"
                        ]
                        row["enumcron-matches"] = [
                            f"{held.raw}→{ht.raw}"
                            for held, ht_set in enumcron_matches.items()
                            for ht in ht_set
                        ]
                    else:
                        selected_items = pick_group(groups)
                    row["htids"] = [item.htid for item in selected_items]
                writer.writerow({key: "; ".join(values) for key, values in row.items()})
    return 0


def pick_volumes(
    items: list[Item],
    holdings: list[str],
    strategy: MatchStrategy,
    exclude_indexes: bool = True,
) -> tuple[list[Item], dict[Enumcron, Enumcron]]:
    if not holdings:
        raise ValueError("no holdings provided")
    if not items:
        raise ValueError("no items to match")

    held_enumcrons = [extract_enumcron(holding) for holding in set(holdings)]
    if exclude_indexes:
        held_enumcrons = [
            held_enumcron
            for held_enumcron in held_enumcrons
            if not held_enumcron.is_index
        ]
    ht_enumcrons = dict()
    for item in items:
        ht_enumcron = extract_enumcron(item.enumcron)
        if ht_enumcron in ht_enumcrons:
            if int(item.lastUpdate) < int(ht_enumcrons[ht_enumcron].lastUpdate):
                continue
        ht_enumcrons[ht_enumcron] = item

    matches = strategy(set(held_enumcrons), set(ht_enumcrons))
    if len(matches) < len(held_enumcrons):
        print(
            f"Missed {len(held_enumcrons) - len(matches)} volumes of {len(held_enumcrons)}",
            file=sys.stderr,
        )
    return (
        [
            ht_enumcrons[ht_enumcron]
            for ht_enumcron in set(itertools.chain.from_iterable(matches.values()))
        ],
        matches,
    )


def match_strategy(argname: str) -> Callable[[MatchStrategy], MatchStrategy]:
    def match_strategy_decorator(func: MatchStrategy) -> MatchStrategy:
        assert (
            argname not in MATCH_STRATEGIES
        ), f"volume match strategy name collision {argname!r}"
        MATCH_STRATEGIES[argname] = func
        return func

    return match_strategy_decorator


@match_strategy("exact")
def exact_match_strategy(
    holdings: set[Enumcron], ht_enumcrons: set[Enumcron]
) -> dict[Enumcron, set[Enumcron]]:
    ht_enumcrons_dict = {ht_enumcron: ht_enumcron for ht_enumcron in ht_enumcrons}
    return {
        holding: {ht_enumcrons_dict[holding]}
        for holding in holdings
        if holding in ht_enumcrons_dict
    }


@match_strategy("1-span")
def single_span_match_strategy(
    holdings: set[Enumcron], ht_enumcrons: set[Enumcron]
) -> dict[Enumcron, set[Enumcron]]:
    return spans_matcher(holdings=holdings, ht_enumcrons=ht_enumcrons, n=1)


@match_strategy("2-span")
def double_span_match_strategy(
    holdings: set[Enumcron], ht_enumcrons: set[Enumcron]
) -> dict[Enumcron, set[Enumcron]]:
    return spans_matcher(holdings=holdings, ht_enumcrons=ht_enumcrons, n=2)


def spans_matcher(
    holdings: set[Enumcron], ht_enumcrons: set[Enumcron], n: int
) -> dict[Enumcron, set[Enumcron]]:
    match_on = guess_mutual_spans(holdings=holdings, ht_enumcrons=ht_enumcrons, n=n)

    holdings_by_key = dict()
    for holding in holdings:
        holdings_by_key.update(make_spans_dict(holding, match_on))

    ht_enumcrons_by_key = dict()
    for ht_enumcron in ht_enumcrons:
        ht_enumcrons_by_key.update(make_spans_dict(ht_enumcron, match_on))

    matches = collections.defaultdict(set)
    for key in set(holdings_by_key) & set(ht_enumcrons_by_key):
        matches[holdings_by_key[key]].add(ht_enumcrons_by_key[key])

    return dict(matches)


def guess_mutual_spans(
    holdings: set[Enumcron], ht_enumcrons: set[Enumcron], n: int
) -> list[str]:
    holdings_span_percents = counts_to_percents(
        filled_span_counts(holdings), len(holdings)
    )
    ht_span_percents = counts_to_percents(
        filled_span_counts(ht_enumcrons), len(ht_enumcrons)
    )
    mutual_span_attrs = set(holdings_span_percents) & set(ht_span_percents)
    mutual_percents = {
        attr: holdings_span_percents[attr] + ht_span_percents[attr]
        for attr in mutual_span_attrs
    }
    sorted_mutual_spans = sorted(
        mutual_percents.items(), key=lambda tpl: tpl[1], reverse=True
    )
    grouped_mutual_spans = itertools.groupby(
        sorted_mutual_spans, key=lambda tpl: int(tpl[1])
    )
    span_groups = [[span[0] for span in group] for _, group in grouped_mutual_spans]
    ordered_span_groups = [
        [span for span in SPAN_ORDER if span in group] for group in span_groups
    ]
    ordered_mutual_spans = itertools.chain.from_iterable(ordered_span_groups)
    result = list(ordered_mutual_spans)[:n]
    return result


def filled_span_counts(enumcrons: Iterable[Enumcron]) -> collections.Counter:
    return collections.Counter(
        attr for enumcron in enumcrons for attr in SPAN_ORDER if getattr(enumcron, attr)
    )


def counts_to_percents(
    counter: collections.Counter, total: int
) -> dict[Hashable, float]:
    return {key: (count / total) * 100 for key, count in counter.items()}


def make_spans_dict(
    enumcron: Enumcron, attrs: Sequence[str]
) -> dict[tuple[Optional[int], ...], Enumcron]:
    return {key: enumcron for key in make_spans_set(enumcron, attrs=attrs)}


def make_spans_set(
    enumcron: Enumcron, attrs: Sequence[str]
) -> set[tuple[Optional[int], ...]]:
    spans = []
    for attr in attrs:
        value = getattr(enumcron, attr)
        if not value:
            spans.append([None])
            continue
        start, end = value
        if isinstance(start, datetime.date) or isinstance(end, datetime.date):
            start, end = start.year, end.year
        spans.append(range(start, end + 1))
    spans_set = set(itertools.product(*spans))
    spans_set.discard((None,) * len(attrs))
    return spans_set


def extract_enumcron(enumcron: str) -> Enumcron:
    if not enumcron:
        return Enumcron()

    asdigit = enumcron.strip(" ()")
    # 1000 is larger than most volumes, smaller than most years
    if asdigit.isdigit() and int(asdigit) < 1000:
        volumenum = int(asdigit)
        return Enumcron(volumespan=(volumenum, volumenum), raw=enumcron)

    remainder = " ".join(enumcron.split())  # Remove multiple spaces
    remainder = translate_to_english(remainder)
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
        raw=enumcron,
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
    remainder, match = search_and_remove(r"(inde?x:?)", enumcron)
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
