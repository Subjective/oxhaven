# Filtering: where with one filter, with several and with none, the filters that take
# another table, and a list of alternatives with one entry; then a list of several; then
# the filters that take another table with no columns to match, which keep every row or
# none by whether the other table has a row.
# oracle: results one, both, none, everything, in_keys, not_in_keys, in_pairs, in_renamed, only_one_of, one_of, in_any, in_none, not_in_any, not_in_none
from deephaven import empty_table

source = empty_table(12).update(["K = i % 4", "V = i * 2", "S = i % 2 == 0 ? `even` : `odd`"])
keys = empty_table(2).update("K = i + 1")
pairs = empty_table(3).update(["Key = i", "S = i == 1 ? `odd` : `even`"])
one = source.where("V >= 10")
both = source.where(["K != 0", "S == `even`"])
none = source.where("V < 0")
everything = source.where()
in_keys = source.where_in(keys, "K")
not_in_keys = source.where_not_in(keys, ["K"])
in_pairs = source.where_in(pairs, ["K = Key", "S"])
in_renamed = source.where_not_in(pairs, "K = Key")
only_one_of = source.where_one_of("K == 3")
# oracle: step
one_of = source.where_one_of(["K == 3", "V < 4"])
# oracle: step
in_any = source.where_in(keys, [])
in_none = source.where_in(keys.head(0), [])
not_in_any = source.where_not_in(keys, [])
not_in_none = source.where_not_in(keys.head(0), [])
