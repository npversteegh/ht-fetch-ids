import pytest

import datetime
import collections

from ht_fetch_ids import ht_fetch_ids as hfi


@pytest.mark.parametrize(
    "strategy,holdings,hts",
    [
        (
            hfi.exact_match_strategy,
            ["v.1", "v.2"],
            {("v.1",): (0,), ("v.3",): (None,)},
        ),
        (
            hfi.single_span_match_strategy,
            ["v.1", "v.2"],
            {("v.1",): (0,), ("v.2",): (1,), ("v.3",): (None,)},
        ),
        (
            hfi.single_span_match_strategy,
            ["v.1", "no.2"],
            {("v.1",): (0,), ("v.2",): (None,), ("no.2",): (None,)},
        ),
        (
            hfi.single_span_match_strategy,
            ["v.1 1991", "v.2 1992"],
            {("v.1",): (0,), ("v.2",): (1,), ("v.3",): (None,)},
        ),
        (
            hfi.single_span_match_strategy,
            ["1991", "1992"],
            {("1991-1992",): (0, 1), ("1993",): (None,)},
        ),
        (
            hfi.double_span_match_strategy,
            ["v.1", "v.2"],
            {("v.1",): (0,), ("v.2",): (1,), ("v.3",): (None,)},
        ),
        (
            hfi.double_span_match_strategy,
            ["v.1 pt.1", "v.1 pt.2"],
            {("v.1 pt.1",): (0,), ("v.1 pt.2",): (1,), ("v.1 pt.3",): (None,)},
        ),
        (
            hfi.double_span_match_strategy,
            ["no.1 pt.1", "no.2 pt.1", "no.2 pt.2"],
            {("no.1 pt.1",): (0,), ("no.2",): (None,), ("no.3",): (None,)},
        ),
    ],
)
def test_match_strategy(strategy, holdings, hts):
    holdings_enumcrons = [hfi.extract_enumcron(holding) for holding in holdings]
    extracted_matches = {
        tuple(hfi.extract_enumcron(ht) for ht in ht_enumcrons): match_indexs
        for ht_enumcrons, match_indexs in hts.items()
    }

    matches = collections.defaultdict(set)
    for ht_enumcrons, match_indexes in extracted_matches.items():
        if len(ht_enumcrons) == 1:
            for index in match_indexes:
                if index is not None:
                    matches[holdings_enumcrons[index]].add(ht_enumcrons[0])
        elif len(match_indexes) == 1:
            for ht_enumcron in ht_enumcrons:
                matches[holdings_enumcrons[index]].add(ht_enumcron)
        else:
            raise ValueError("malformed many-to-many test case")
    matches = dict(matches)

    ht_enumcrons_set = set()
    for ht_enumcrons in extracted_matches:
        ht_enumcrons_set.update(ht_enumcrons)

    assert strategy(holdings_enumcrons, ht_enumcrons_set) == matches


@pytest.mark.parametrize(
    "holdings,hts,n,result",
    [
        (["v.1", "v.2"], ["v.1", "v.2"], 1, ["volumespan"]),
        (["v.1", "v.2"], ["v.1", "no.2"], 1, ["volumespan"]),
        (["v.1", "v.2"], ["no.1", "no.2"], 1, []),
        (["v.1", "v.2", "no.3"], ["v.1", "no.2"], 1, ["volumespan"]),
        (["v.1 pt.1", "v.1 pt.2"], ["v.1 pt.1", "v.2 pt.2"], 1, ["volumespan"]),
        (
            ["v.1 pt.1", "v.1 pt.2"],
            ["v.1 pt.1", "v.2 pt.2"],
            2,
            ["volumespan", "partspan"],
        ),
        (["2001", "2002"], ["1998", "1999"], 1, ["datespan"]),
        (["v.1 2002", "v.2 2003"], ["v.1", "2002"], 2, ["volumespan", "datespan"]),
    ],
)
def test_guess_mutual_spans(holdings, hts, n, result):
    assert (
        hfi.guess_mutual_spans(
            holdings=[hfi.extract_enumcron(holding) for holding in holdings],
            ht_enumcrons=[hfi.extract_enumcron(ht) for ht in hts],
            n=n,
        )
        == result
    )


@pytest.mark.parametrize(
    "enumcron,attrs,result",
    [
        (hfi.Enumcron(volumespan=(1, 2)), ["volumespan"], {(1,), (2,)}),
        (
            hfi.Enumcron(volumespan=(1, 1), partspan=(1, 1)),
            ["volumespan", "partspan"],
            {(1, 1)},
        ),
        (hfi.Enumcron(volumespan=(1, 1)), ["volumespan", "partspan"], {(1, None)},),
        (
            hfi.Enumcron(
                datespan=(datetime.date(2000, 1, 1), datetime.date(2001, 12, 31)),
            ),
            ["datespan"],
            {(2000,), (2001,)},
        ),
    ],
)
def test_make_spans_set(enumcron, attrs, result):
    assert hfi.make_spans_set(enumcron, attrs=attrs) == result


@pytest.mark.parametrize(
    "enumcrons,count",
    [
        ([hfi.Enumcron(volumespan=(1, 2))], {"volumespan": 1}),
        (
            [
                hfi.Enumcron(numberspan=(1, 1)),
                hfi.Enumcron(volumespan=(1, 1), numberspan=(2, 2)),
            ],
            {"volumespan": 1, "numberspan": 2},
        ),
    ],
)
def test_filled_span_counts(enumcrons, count):
    assert dict(hfi.filled_span_counts(enumcrons)) == count


@pytest.mark.parametrize(
    "raw,extracted",
    [
        ("v.1", hfi.Enumcron(volumespan=(1, 1))),
        ("V. 123", hfi.Enumcron(volumespan=(123, 123))),
        ("NO. 1", hfi.Enumcron(numberspan=(1, 1))),
        ("pt. 1", hfi.Enumcron(partspan=(1, 1))),
        (
            "1900",
            hfi.Enumcron(
                datespan=(datetime.date(1900, 1, 1), datetime.date(1900, 12, 31))
            ),
        ),
        (
            "1926-27",
            hfi.Enumcron(
                datespan=(datetime.date(1926, 1, 1), datetime.date(1927, 12, 31))
            ),
        ),
        (
            "2002-2003",
            hfi.Enumcron(
                datespan=(datetime.date(2002, 1, 1), datetime.date(2003, 12, 31))
            ),
        ),
        (
            "v.1, 1900",
            hfi.Enumcron(
                volumespan=(1, 1),
                datespan=(datetime.date(1900, 1, 1), datetime.date(1900, 12, 31)),
            ),
        ),
    ],
)
def test_extract_enumcron(raw, extracted):
    assert hfi.extract_enumcron(raw) == extracted


@pytest.mark.parametrize(
    "raw,zapped",
    [
        ("c.1 v.2", "v.2"),
        ("copy 2", False),
        ("[copy 2]", False),
        ("c.2", False),
        ("Dec 1999", "Dec 1999"),
    ],
)
def test_zap_copy_number(raw, zapped):
    assert hfi.zap_copy_number(raw) == zapped


@pytest.mark.parametrize(
    "raw,extracted",
    [
        ("1234-456x", ("1234-456x", "issn")),
        ("123456789x", ("123456789x", "isbn")),
        ("1234567890 extra", ("1234567890", "isbn")),
        ("before 123456789012x", ("123456789012x", "isbn")),
    ],
)
def test_search_for_isn(raw, extracted):
    assert hfi.search_for_isn(raw) == extracted


@pytest.mark.parametrize(
    "values,results",
    [
        ('"1"', ["1"]),
        ('"1";"2";"3"', ["1", "2", "3"]),
        ("", []),
        ('"1;";"2"', ["1;", "2"]),
    ],
)
def test__split_repeated_values(values, results):
    assert (
        hfi._split_repeated_values(
            values, text_qualifier='"', repeated_field_delimiter=";"
        )
        == results
    )
