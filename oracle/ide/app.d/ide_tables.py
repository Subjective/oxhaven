"""The two tables of the IDE acceptance test, as the Java reference builds them.

`expected.json` describes the same tables for the test. A server under test has to offer
tables of these names with these contents; how it builds them is its own business.
"""


def make():
    from deephaven import new_table, time_table
    from deephaven.column import double_col, int_col, string_col

    static = new_table(
        [
            int_col("Id", [1, 2, 3, 4, 5]),
            string_col("Sym", ["AAPL", "MSFT", "GOOG", "AMZN", "OXHV"]),
            double_col("Price", [101.5, 202.25, 303.125, 404.0, 505.75]),
        ]
    )
    # Three rows whose values all change twice a second: the last three ticks of a counter.
    ticking = time_table("PT0.5S").update(["N = ii", "Sq = ii * ii"]).tail(3)
    return static, ticking
