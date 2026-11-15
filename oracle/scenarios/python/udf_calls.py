# Which callable a call is, and when and how often it is called: a function that counts its
# calls, a function of the calling function's scope, a callable object, a partial, and a
# name the function library has too.
# oracle: results counted, counted_again, counted_where, counted_total, local, instance, bound, library, python_sqrt, lazy, before, after
import functools

from deephaven import empty_table

calls = 0


def numbered() -> int:
    global calls
    calls += 1
    return calls


def numbered_from(x) -> int:
    global calls
    calls += 1
    return calls


def in_function():
    def local_function(x) -> int:
        return x + 1000

    return source.update("A = local_function(X)")


class Adder:
    def __init__(self, amount):
        self.amount = amount

    def __call__(self, x) -> int:
        return x + self.amount


def add(amount, x) -> int:
    return x + amount


def abs(x) -> str:
    return "python"


def sqrt(text: str) -> str:
    return "python " + text


def plus_one(x) -> int:
    return x + 1


add_five = Adder(5)
add_seven = functools.partial(add, 7)

source = empty_table(4).update(["X = i", "T = `s` + i"])
# each row is called once, in row order
counted = source.update("N = numbered()")
counted_again = source.select("N = numbered_from(X + 0)")
counted_where = source.where("numbered_from(X) % 2 == 0")
counted_total = empty_table(1).update("N = numbered()")
local = in_function()
instance = source.update("A = add_five(X)")
bound = source.update("A = add_seven(X)")
# the library's abs takes an int, so it is the one called; its sqrt takes no String
library = source.update("A = abs(X - 2)")
python_sqrt = source.update("A = sqrt(T)")
lazy = source.view("A = plus_one(X)").update_view("B = plus_one(A)").lazy_update("C = plus_one(B)")
# oracle: step
# A name assigned again does not change a formula compiled before.


def renamed(x) -> int:
    return 1


before = source.update_view("A = renamed(X)")


def renamed(x) -> int:
    return 2


after = source.update_view("A = renamed(X)")
