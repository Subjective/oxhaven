# Calls of Python functions that fail: an argument the parameter's annotation does not take,
# an exception in the function, a filter that is not a boolean, a column that hides the
# function, and a null condition.
# oracle: results source, made
from typing import Optional

from deephaven import empty_table


def takes_int(x: int) -> int:
    return x


def takes_float(x: float) -> float:
    return x


def takes_str(x: str) -> str:
    return x


def takes_bool(x: bool) -> bool:
    return x


def takes_nothing() -> int:
    return 1


def takes_list(x: list) -> int:
    return 1


def raises(x) -> int:
    raise ValueError("no " + str(x))


def maybe(x) -> Optional[bool]:
    return None if x == 1 else x > 1


def hidden(x) -> int:
    return x


source = empty_table(4).update(["X = i", "D = i * 1.5", "T = `s` + i"])
# oracle: fails
made = source.update("A = takes_int(D)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int(T)")
# oracle: step
# oracle: fails
made = source.update("A = takes_float(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_str(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_bool(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int(null)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int(1.5)")
# oracle: step
# oracle: fails
made = source.update("A = takes_nothing(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_list(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int(X, X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int()")
# oracle: step
# oracle: fails
made = source.update("A = raises(X)")
# oracle: step
# oracle: fails
made = source.select("A = raises(X)")
# oracle: step
# oracle: fails
made = source.where("raises(X) > 0")
# oracle: step
# oracle: fails
made = source.where("takes_int(X)")
# oracle: step
# oracle: fails
made = source.where("maybe(X) && X > 0")
# oracle: step
# oracle: fails
made = source.where("!maybe(X)")
# oracle: step
# oracle: fails
made = source.update("hidden = X").update("A = hidden(X)")
# oracle: step
# oracle: fails
made = source.update("A = no_such_function(X)")
# oracle: step
# oracle: fails
made = source.update("A = takes_int('a')")
# oracle: step
# After the failures a call still works.
made = source.update("A = takes_int(X)")
