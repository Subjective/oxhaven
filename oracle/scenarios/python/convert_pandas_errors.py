# What the conversions to and from pandas refuse. The table made first is there after each
# refusal.
# oracle: results kept
# oracle: fails
import pandas as pd

from deephaven import empty_table
from deephaven.pandas import to_pandas, to_table

kept = empty_table(2).update(["X = i", "S = `a`"])
to_pandas(kept, conv_null=False)
# oracle: step
# oracle: fails
to_pandas(kept, dtype_backend="pyarrow", conv_null=False)
# oracle: step
# oracle: fails
to_pandas(kept, cols=["Nope"])
# oracle: step
# oracle: fails
to_pandas(kept, cols=["Nope"], dtype_backend=None)
# oracle: step
# oracle: fails
to_pandas(5)
# oracle: step
# oracle: fails
to_pandas(5, dtype_backend=None)
# oracle: step
# oracle: fails
to_table(pd.DataFrame({"A": [1]}), cols=["Nope"])
# oracle: step
# oracle: fails
to_table(pd.DataFrame({"A": [1], "1x": [2]}))
# oracle: step
# oracle: fails
to_table([1])
# oracle: step
# oracle: fails
to_table(pd.DataFrame([[1, 2]], columns=["A", "A"]))
# oracle: step
# oracle: fails
to_table(pd.DataFrame({"A": [1], "B": [2]}), cols="B")
