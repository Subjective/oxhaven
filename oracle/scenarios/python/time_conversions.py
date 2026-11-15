# deephaven.time: Python, numpy and pandas values to the engine's time values and back, each
# as its text.
# oracle: results to_java, to_python
import datetime

import numpy as np
import pandas as pd

from deephaven import new_table
from deephaven.column import string_col
from deephaven.time import (
    to_date, to_datetime, to_j_duration, to_j_instant, to_j_local_date, to_j_local_time,
    to_j_period, to_j_time_zone, to_j_zdt, to_np_datetime64, to_np_timedelta64, to_pd_timedelta,
    to_pd_timestamp, to_time, to_timedelta,
)

utc = datetime.timezone.utc
calls = [
    ("time zone of text", to_j_time_zone, "America/Chicago"),
    ("time zone of an alias", to_j_time_zone, "ET"),
    ("time zone of an offset", to_j_time_zone, "+05:30"),
    ("time zone of UTC", to_j_time_zone, "UTC"),
    ("time zone of a tzinfo", to_j_time_zone, datetime.timezone(datetime.timedelta(hours=2))),
    ("time zone of a datetime", to_j_time_zone, datetime.datetime(2024, 1, 2, tzinfo=utc)),
    ("time zone of a Timestamp", to_j_time_zone, pd.Timestamp("2024-01-02", tz="Asia/Tokyo")),
    ("local date of text", to_j_local_date, "2024-02-29"),
    ("local date of a day that is not", to_j_local_date, "2023-02-29"),
    ("local date of a date", to_j_local_date, datetime.date(2024, 2, 29)),
    ("local date of a datetime", to_j_local_date, datetime.datetime(2024, 2, 29, 23, 59)),
    ("local date of a datetime64", to_j_local_date, np.datetime64("2024-02-29T23:59")),
    ("local date of a Timestamp", to_j_local_date, pd.Timestamp("2024-02-29T23:59")),
    ("local time of text", to_j_local_time, "12:34:56.789"),
    ("local time of hours and minutes", to_j_local_time, "01:02"),
    ("local time of nanoseconds", to_j_local_time, "23:59:59.999999999"),
    ("local time of a time", to_j_local_time, datetime.time(1, 2, 3, 4)),
    ("local time of a datetime", to_j_local_time, datetime.datetime(2024, 1, 2, 3, 4, 5, 6)),
    ("local time of a Timestamp", to_j_local_time, pd.Timestamp("2024-01-02T03:04:05.000000007")),
    ("instant of text", to_j_instant, "2024-03-10T01:30:00 ET"),
    ("instant of a number", to_j_instant, 1704164645123456789),
    ("instant of a datetime", to_j_instant, datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc)),
    ("instant of a datetime64", to_j_instant, np.datetime64("2024-01-02T03:04:05.123456789")),
    ("instant of a Timestamp", to_j_instant, pd.Timestamp("2024-01-02T03:04:05.123456789", tz="UTC")),
    ("zoned of text", to_j_zdt, "2024-03-10T03:30:00 ET"),
    ("zoned of text in the hour that is not", to_j_zdt, "2024-03-10T02:30:00 ET"),
    ("zoned of text in the hour that is twice", to_j_zdt, "2024-11-03T01:30:00 ET"),
    ("zoned of text with an offset", to_j_zdt, "2024-01-02T03:04:05+01:00"),
    ("zoned of a datetime", to_j_zdt, datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc)),
    ("zoned of a Timestamp", to_j_zdt, pd.Timestamp("2024-07-02T03:04:05", tz="Europe/Paris")),
    ("duration of text", to_j_duration, "PT1H30M"),
    ("duration of negative text", to_j_duration, "PT-1.5S"),
    ("duration of days", to_j_duration, "P2DT3H4M5.006S"),
    ("duration of a number", to_j_duration, 1_500_000_000),
    ("duration of a timedelta", to_j_duration, datetime.timedelta(days=1, microseconds=5)),
    ("duration of a negative timedelta", to_j_duration, datetime.timedelta(microseconds=-1)),
    ("duration of a timedelta64", to_j_duration, np.timedelta64(1500, "ms")),
    ("duration of a Timedelta", to_j_duration, pd.Timedelta("1 days 00:00:00.000000005")),
    ("period of text", to_j_period, "P1Y2M3D"),
    ("period of weeks", to_j_period, "P2W"),
    ("period of negative text", to_j_period, "-P1M"),
    ("period of a timedelta", to_j_period, datetime.timedelta(days=45)),
    ("period of a timedelta64", to_j_period, np.timedelta64(3, "D")),
    ("period of a Timedelta", to_j_period, pd.Timedelta(days=10)),
]
to_java = new_table([
    string_col("Call", [name for name, _, _ in calls] + ["of None"]),
    string_col("Text", [str(convert(value)) for _, convert, value in calls] + [str([
        convert(None) for convert in (
            to_j_time_zone, to_j_local_date, to_j_local_time, to_j_instant, to_j_zdt,
            to_j_duration, to_j_period)])]),
])

instant = to_j_instant("2024-01-02T03:04:05.123456789 UTC")
zoned = to_j_zdt("2024-07-02T03:04:05.123456789 ET")
duration = to_j_duration("PT25H0.000001001S")
back = [
    ("date of a local date", to_date, to_j_local_date("2024-02-29")),
    ("date of a zoned", to_date, zoned),
    ("time of a local time", to_time, to_j_local_time("12:34:56.789123456")),
    ("time of a zoned", to_time, zoned),
    ("datetime of an instant", to_datetime, instant),
    ("datetime of a zoned", to_datetime, zoned),
    ("Timestamp of an instant", to_pd_timestamp, instant),
    ("Timestamp of a zoned", to_pd_timestamp, zoned),
    ("datetime64 of an instant", to_np_datetime64, instant),
    ("datetime64 of a zoned", to_np_datetime64, zoned),
    ("timedelta of a duration", to_timedelta, duration),
    ("timedelta of a period", to_timedelta, to_j_period("P3D")),
    ("Timedelta of a duration", to_pd_timedelta, duration),
    ("Timedelta of a period", to_pd_timedelta, to_j_period("P3D")),
    ("timedelta64 of a duration", to_np_timedelta64, duration),
    ("timedelta64 of a period", to_np_timedelta64, to_j_period("P3D")),
]
to_python = new_table([
    string_col("Call", [name for name, _, _ in back] + ["of None"]),
    string_col("Type", [type(convert(value)).__name__ for _, convert, value in back] + ["list"]),
    string_col("Text", [str(convert(value)) for _, convert, value in back] + [str([
        convert(None) for convert in (
            to_date, to_time, to_datetime, to_pd_timestamp, to_np_datetime64, to_timedelta,
            to_pd_timedelta, to_np_timedelta64)])]),
])
