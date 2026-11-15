# Time zones: the aliases the engine starts with, one added and removed, and the current
# time and date.
# oracle: results aliases, added, clock
import datetime

from deephaven import new_table
from deephaven.column import bool_col, string_col
from deephaven.time import (
    dh_now, dh_time_zone, dh_today, time_zone_alias_add, time_zone_alias_rm, to_j_instant,
    to_j_time_zone,
)

names = ["ET", "CT", "MT", "PT", "HK", "JP", "LON", "UTC", "GMT", "UT", "Europe/Paris",
         "UTC+01:00", "GMT-5", "-18:00", "Etc/GMT+3"]
aliases = new_table([
    string_col("Name", names),
    string_col("Zone", [str(to_j_time_zone(name)) for name in names]),
    string_col("Noon", [str(to_j_instant("2024-07-01T12:00:00 " + name)) for name in names]),
])
time_zone_alias_add("PAR", "Europe/Paris")
try:
    during = [str(to_j_time_zone("PAR")), str(to_j_instant("2024-01-02T09:00:00 PAR"))]
finally:
    removed = time_zone_alias_rm("PAR")
added = new_table([string_col("Text", during + [
    str(removed), str(time_zone_alias_rm("PAR")), str(to_j_time_zone("Z"))])])

before = datetime.datetime.now(datetime.timezone.utc)
now = dh_now()
today = dh_today(to_j_time_zone("UTC"))
after = datetime.datetime.now(datetime.timezone.utc)
clock = new_table([
    string_col("Check", ["now is the time of the clock", "now of the system clock is too",
                         "today is the date of now", "today has no zone to name",
                         "the zone of the server has a name"]),
    bool_col("Holds", [
        before.timestamp() - 1 <= now.toEpochMilli() / 1000 <= after.timestamp() + 1,
        abs(dh_now(system=True, resolution="ms").toEpochMilli() / 1000 - after.timestamp()) < 60,
        today in (str(before.date()), str(after.date())),
        len(dh_today()) == 10,
        str(dh_time_zone()) == str(to_j_time_zone(None)) != "",
    ]),
])
