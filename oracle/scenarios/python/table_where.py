# Filtering: where with one filter and with several, and the filters that take another
# table or a list of alternatives.
# oracle: results one, both, none, in_keys, not_in_keys, one_of
from deephaven import empty_table

source = empty_table(12).update(["K = i % 4", "V = i * 2", "S = i % 2 == 0 ? `even` : `odd`"])
keys = empty_table(2).update("K = i + 1")
one = source.where("V >= 10")
both = source.where(["K != 0", "S == `even`"])
none = source.where("V < 0")
in_keys = source.where_in(keys, "K")
not_in_keys = source.where_not_in(keys, ["K"])
one_of = source.where_one_of(["K == 3", "V < 4"])
