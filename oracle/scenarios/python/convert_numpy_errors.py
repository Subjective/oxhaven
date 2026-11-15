# What the conversions to and from numpy refuse. The table made first is there after each
# refusal.
# oracle: results kept
# oracle: fails
import numpy as np

from deephaven import empty_table
from deephaven.numpy import to_numpy, to_table

kept = empty_table(2).update(["X = i", "S = `a`"])
to_numpy(kept)
# oracle: step
# oracle: fails
to_numpy(kept, ["Nope"])
# oracle: step
# oracle: fails
to_numpy(empty_table(2))
# oracle: step
# oracle: fails
to_numpy(5)
# oracle: step
# oracle: fails
to_table(np.array([[1, 2], [3, 4]]), ["X"])
# oracle: step
# oracle: fails
to_table(np.array([1, 2]), ["X", "Y"])
# oracle: step
# oracle: fails
to_table(np.array([1, 2, 3]), ["X"])
# oracle: step
# oracle: fails
to_table(np.zeros((2, 2, 2)), ["X", "Y"])
# oracle: step
# oracle: fails
to_table([1, 2], ["X"])
# oracle: step
# oracle: fails
to_table(np.array([[1, 2]]), ["A", "1b"])
