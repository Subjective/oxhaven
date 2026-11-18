"""record.py <probe_server lines> <out> <probe_figure lines>: the per-year summaries, a
sample of function answers and the holiday lists of Deephaven 42.9's shipped calendars,
as the expectations of oxhaven-expr's tests."""
import ast
import datetime
import sys

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
FNV_OFFSET, FNV_PRIME, MASK = 0xCBF29CE484222325, 0x100000001B3, (1 << 64) - 1
SAMPLES = 40
KINDS = ("nbd", "nbdx", "nnbd", "next", "prev", "plus", "minus", "plusnb", "frac", "lastm", "lastw",
         "bt", "fbdc", "dbn", "dby", "plusI")


def nanos(text):
    assert text.endswith("Z"), text
    body = text[:-1]
    fraction = 0
    if "." in body:
        body, digits = body.split(".")
        fraction = int(digits.ljust(9, "0"))
    moment = datetime.datetime.fromisoformat(body).replace(tzinfo=datetime.timezone.utc)
    delta = moment - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + fraction


def fnv(state, text):
    for byte in text.encode():
        state = ((state ^ byte) * FNV_PRIME) & MASK
    return state


years = {}
out = ["# Recorded from Deephaven 42.9 by running the same calls on its shipped calendars.",
       "# year|calendar|year|business days|business nanos|FNV-1a 64 of the days' lines",
       "# sample|calendar|index|a|b|n|kind|answer",
       "# holidays|calendar|count|FNV-1a 64 of the lines <date>|<open>-<close>;... of holidays()"]
for line in open(sys.argv[1], encoding="utf-8"):
    p = line.rstrip("\n").split("|")
    if len(p) >= 4 and p[1] == "day":
        day = datetime.date.fromisoformat(p[2])
        epoch_day = (day - datetime.date(1970, 1, 1)).days
        text = f"{epoch_day}:{p[3]}:"
        total = 0
        for part in filter(None, p[4].split(";")):
            inclusive = part.endswith("i")
            start, end = part.rstrip("i").split("~")
            s, e = nanos(start), nanos(end)
            text += f"{s},{e},{int(inclusive)};"
            total += e - s + (1 if inclusive else 0)
        key = (p[0], day.year)
        count, sum_nanos, state = years.get(key, (0, 0, FNV_OFFSET))
        years[key] = (count + (p[3] == "B"), sum_nanos + total, fnv(state, text + "\n"))
    elif len(p) == 7 and p[1].startswith("s") and p[1][1:].isdigit():
        if int(p[1][1:]) < SAMPLES and p[5] in KINDS:
            out.append("sample|" + "|".join([p[0], p[1][1:]] + p[2:]))
for (calendar, year), (count, total, state) in sorted(years.items()):
    out.append(f"year|{calendar}|{year}|{count}|{total}|{state:016x}")

# holidays|calendar|count|FNV-1a 64 of "<date>|<open>-<close>;...\n" per holiday, in order
# of date, as FigureCalendar reads the holidays (times of day in the calendar's zone);
# from the lines of probe_figure.py
holidays = {}
for line in open(sys.argv[3], encoding="utf-8"):
    parts = line.rstrip("\n").split("|", 2)
    if len(parts) != 3 or parts[1] != "holiday":
        continue
    name, _, body = parts
    holiday = ast.literal_eval(body)
    periods = "".join(f"{p['open']}-{p['close']};" for p in holiday["businessPeriods"])
    count, state = holidays.get(name, (0, FNV_OFFSET))
    holidays[name] = (count + 1, fnv(state, holiday["date"] + "|" + periods + "\n"))
for name, (count, state) in sorted(holidays.items()):
    out.append(f"holidays|{name}|{count}|{state:016x}")
open(sys.argv[2], "w", encoding="utf-8").write("\n".join(out) + "\n")
