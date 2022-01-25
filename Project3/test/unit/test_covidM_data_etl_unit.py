import datetime

from covidmonitor.covidM_data_etl import get_datetime


def test_get_datetime():
    actual_dt: str = "2020-04-05T06:37:00Z"
    expected_dt = datetime.datetime(
        2020, 4, 5, 6, 37, tzinfo=datetime.timezone.utc
    )
    assert expected_dt == get_datetime(actual_dt)
