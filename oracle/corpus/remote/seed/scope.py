# The scope of the remote, made once when it has started (harness remote-seed). The remote
# scenarios read these tables, and make the ticking ones they change themselves.
#
# oracle_types: a column of every type the remote can make that a Barrage client reads, from
# an int column N (not `i`, so that it also serves a refreshing table). BigDecimal,
# BigInteger and arrays of Instant are not here: Oxhaven's formulas cannot make them
# (unsupported static and array calls), so an Oxhaven remote never serves them.
from deephaven import empty_table

oracle_types = [
    "Bo = N % 3 == 0 ? null : (Boolean)(N % 2 == 0)",
    "By = N % 7 == 0 ? NULL_BYTE : (byte)(N % 100 - 50)",
    "Ch = N % 5 == 0 ? NULL_CHAR : (char)('a' + N % 26)",
    "Sh = N % 11 == 0 ? NULL_SHORT : (short)(N * 3 - 1000)",
    "In = N % 13 == 0 ? NULL_INT : N * 7 - 2000",
    "Lo = N % 17 == 0 ? NULL_LONG : (long)N * 1000000007L - 5000000000L",
    "Fl = N % 19 == 0 ? NULL_FLOAT : (float)(N / 3.0 - 100)",
    "Do = N % 23 == 0 ? NULL_DOUBLE : N / 7.0 - 50",
    "St = N % 4 == 0 ? null : `s` + N",
    "Ti = N % 6 == 0 ? null : '2024-01-01T00:00:00Z' + N * 1000000007L",
    "Date = N % 6 == 1 ? null : toLocalDate('2024-01-01T00:00:00Z' + N * 7 * DAY, 'UTC')",
    "Time = N % 6 == 2 ? null : toLocalTime('2024-01-01T00:00:00Z' + N * 1000000007L, 'UTC')",
    "Dur = N % 6 == 3 ? null : N % 2 == 0 ? 'PT1s' : 'PT2h3m4.5s'",
    "Per = N % 6 == 4 ? null : N % 2 == 0 ? 'P1Y2M3D' : 'P5M'",
    "Zdt = N % 6 == 5 ? null : toZonedDateTime('2024-01-01T00:00:00Z' + N * 1000000007L, 'America/New_York')",
    "Bytes = N % 9 == 0 ? null : new byte[]{(byte)N, (byte)-N}",
    "Chars = N % 9 == 1 ? null : new char[]{(char)('a' + N % 26), NULL_CHAR}",
    "Shorts = N % 9 == 2 ? null : new short[]{(short)N, NULL_SHORT}",
    "Ints = N % 9 == 3 ? null : new int[]{N, NULL_INT, N * 2}",
    "Longs = N % 9 == 4 ? null : new long[]{(long)N * 1099511627776L}",
    "Floats = N % 9 == 5 ? null : new float[]{(float)(N / 4.0), NULL_FLOAT}",
    "Doubles = N % 9 == 6 ? null : new double[]{N / 2.0, NULL_DOUBLE}",
    "Strings = N % 9 == 7 ? null : new String[]{`a` + N, null, ``}",
]

types_t = empty_table(40).update(["N = (int)i"]).update(oracle_types)

vectors_t = empty_table(60).update([
    "G = i % 6", "I = i % 4 == 0 ? NULL_INT : (int)i", "L = (long)i * 3", "D = i / 2.0",
    "F = (float)(i / 4.0)", "B = (byte)i", "H = (short)(i - 30)", "C = (char)('a' + i % 26)",
    "S = i % 5 == 0 ? null : `s` + i", "T = i % 7 == 0 ? null : '2024-01-01T00:00:00Z' + i * SECOND",
]).group_by("G")

# A table without rows, of the same columns.
empty_t = types_t.head(0)
