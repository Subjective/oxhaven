"""explain.py <diff.txt>: every differing sample answer must have a differing day within
its reach (the dates between its ends, plus a margin for counts of days, plus whole years
for business years). Prints the samples that are not explained."""
import datetime
import sys

days = {}
samples = []
for line in open(sys.argv[1], encoding="utf-8"):
    if not line.startswith("A: "):
        continue
    p = line[3:].rstrip("\n").split("|")
    if len(p) > 2 and p[1] == "day":
        days.setdefault(p[0], []).append(datetime.date.fromisoformat(p[2]))
    elif len(p) == 7 and p[1].startswith("s"):
        samples.append(p)
unexplained = 0
for p in samples:
    name, _, a, b, n, kind, _ = p
    da, db = datetime.date.fromisoformat(a[:10]), datetime.date.fromisoformat(b[:10])
    low, high = min(da, db), max(da, db)
    if kind == "dby":
        low, high = datetime.date(low.year, 1, 1), datetime.date(high.year, 12, 31)
    reach = datetime.timedelta(days=abs(int(n)) * 3 + 10)
    if not any(low - reach <= d <= high + reach for d in days.get(name, [])):
        unexplained += 1
        print("unexplained:", "|".join(p))
print(len(samples), "differing samples,", unexplained, "unexplained")
