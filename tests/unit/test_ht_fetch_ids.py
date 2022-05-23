import pytest

import datetime

from ht_fetch_ids import ht_fetch_ids as hfi


@pytest.mark.parametrize(
    "holdings,hts",
    [
        (["v.1", "v.2"], {"v.1": True, "v.2": True, "v.3": False}),
        (["v.1 1991", "v.2 1992"], {"v.1": True, "v.2": True, "v.3": False}),
        (["1991", "1992"], {"1991-1992": True, "1993": False}),
    ],
)
def test_single_range_match_strategy(holdings, hts):
    holdings_enumcrons = [hfi.extract_enumcron(holding) for holding in holdings]
    ht_enumcrons = {hfi.extract_enumcron(ht): match for ht, match in hts.items()}

    assert hfi.single_range_match_strategy(holdings_enumcrons, list(ht_enumcrons)) == {
        ht_enumcron for ht_enumcron, match in ht_enumcrons.items() if match
    }


@pytest.mark.parametrize(
    "holdings,hts",
    [
        (["v.1", "v.2"], {"v.1": True, "v.2": True, "v.3": False}),
        (
            ["v.1 pt.1", "v.1 pt.2"],
            {"v.1 pt.1": True, "v.1 pt.2": True, "v.1 pt.3": False},
        ),
        (
            ["no.1 pt.1", "no.2 pt.1", "no.2 pt.2"],
            {"no.1 pt.1": True, "no.2": False, "no.3": False},
        ),
    ],
)
def test_double_range_match_strategy(holdings, hts):
    holdings_enumcrons = [hfi.extract_enumcron(holding) for holding in holdings]
    ht_enumcrons = {hfi.extract_enumcron(ht): match for ht, match in hts.items()}

    assert hfi.double_range_match_strategy(holdings_enumcrons, list(ht_enumcrons)) == {
        ht_enumcron for ht_enumcron, match in ht_enumcrons.items() if match
    }


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
def test_make_range_set(enumcron, attrs, result):
    assert hfi.make_range_set(enumcron, attrs=attrs) == result


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
def test_filled_range_counts(enumcrons, count):
    assert dict(hfi.filled_range_counts(enumcrons)) == count
