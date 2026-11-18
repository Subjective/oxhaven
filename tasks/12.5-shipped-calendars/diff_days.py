"""diff_days.py <a.txt> <b.txt>: prints every observation line that differs, grouped."""
import sys

def load(path):
    found = {}
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        parts = line.split("|")
        if len(parts) >= 3 and parts[1] == "day":
            key = "|".join(parts[:3])
        elif len(parts) >= 6 and parts[1].startswith("s") and parts[1][1:].isdigit():
            key = "|".join(parts[:6])
        else:
            key = "|".join(parts[:2])
        found[key] = line
    return found

a, b = load(sys.argv[1]), load(sys.argv[2])
diffs = [k for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)]
for k in diffs:
    print("A:", a.get(k))
    print("B:", b.get(k))
print(len(diffs), "differences", file=sys.stderr)
