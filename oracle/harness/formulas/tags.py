"""Classifies a formula: inside or outside the accepted subset, and which features it uses.

No Java counterpart: this is a syntactic reading of the formula text, made so that later tasks
can gate on parts of the corpus. It does not type-check and it does not decide what Java does;
the recorded outcome does that.

The accepted subset is what can be written without arbitrary Java: operators, literals, casts to
primitive types (and to `String` or `Boolean`), the ternary operator, column references, column
arrays (`Col_`, indexing), the row variables `i`, `ii` and `k`, and calls to the functions and
constants a formula imports by default (`builtins.json`). A formula is outside the subset when it
calls a method on a value (`s.length()`), constructs an object (`new ...`), names a class
(`Math.abs(x)`, `Integer.MAX_VALUE`), casts to another class, uses `instanceof`, a lambda, an
array literal or an assignment, or names a function or variable that is neither a column nor a
default import (which in Java means a user class, a query scope variable or a Groovy or Python
function).

Tags:

    arith            + - * / % (binary or unary; `+` on strings is tagged the same way)
    compare          == != < <= > >=
    logic            && || !
    bitwise          & | ^ ~ (on Boolean operands Java treats & | ^ as logic without short cut)
    shift            << >> >>>
    ternary          c ? a : b
    cast             (type) x
    string           a string literal, or a String column
    char             a char literal, or a char column
    time-literal     a quoted literal that is not one char, for example '2024-01-01T00:00:00 NY'
    null             the `null` literal, a NULL_* constant, or `isNull`
    null-literal     the `null` literal itself
    constant         a default-imported constant (NULL_INT, MAX_LONG, NAN_DOUBLE, SECOND, ...)
    function:<name>  a call to a default-imported function
    array            a column array (`Col_`) or indexing with `[...]`
    row-var          `i`, `ii` or `k`
    type:<type>      the type of a column the formula reads (byte, short, int, long, float,
                     double, char, Boolean, String, Instant, or the fixture type for others)
    nondeterministic a function whose result changes between runs (random numbers, the clock)
    minimal          accepted, and uses only column references, numeric, boolean, string and
                     char literals, + - * / %, comparisons, && || !, parentheses and `isNull`
    filter:match     (where only) `Col == literal`, `Col != literal`, or an `in` filter
    filter:range     (where only) `Col < literal` and the like
    filter:condition (where only) any other filter
    in               (where only) `Col [icase] [not] in v1, v2, ...`

and, for formulas outside the subset, the reasons: `method-call` and `method:<name>`, `new`,
`class-ref`, `field-access`, `cast-object`, `instanceof`, `lambda`, `array-literal`,
`assignment`, `unknown-function`, `unknown-variable`.

The `filter:*` tags are a syntactic approximation of
io.deephaven.engine.table.impl.select.WhereFilterFactory#getExpression, which decides in Java
whether a filter string becomes a MatchFilter, a RangeFilter or a ConditionFilter.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

PRIMITIVES = ("byte", "short", "int", "long", "float", "double", "char", "boolean")
ROW_VARS = ("i", "ii", "k")
# Casting to these two classes stays inside the subset.
ACCEPTED_CAST_CLASSES = ("String", "Boolean")

# Fixture column type -> the name used in `type:` tags.
TYPE_TAGS = {
    "bool": "Boolean", "int8": "byte", "int16": "short", "int32": "int", "int64": "long",
    "float32": "float", "float64": "double", "char": "char", "string": "String",
    "timestamp": "Instant",
}

NONDETERMINISTIC = re.compile(
    r"^(random\w*|now\w*|today\w*|currentClock|timeZone|nowSystem\w*|todaySystem\w*)$"
)

_TOKEN = re.compile(
    r"""
    (?P<space>\s+)
  | (?P<number>0[xX][0-9a-fA-F_]+[lL]?|0[bB][01_]+[lL]?
        |(?:\d[\d_]*\.?[\d_]*|\.\d[\d_]*)(?:[eE][+-]?\d+)?[fFdDlL]?)
  | (?P<ident>[A-Za-z_$][A-Za-z0-9_$]*)
  | (?P<string>"(?:\\.|[^"\\])*")
  | (?P<backtick>`[^`]*`)
  | (?P<quote>'(?:\\.|[^'\\])*')
  | (?P<op>>>>=|>>>|<<=|>>=|==|!=|<=|>=|&&|\|\||\+\+|--|->|::|<<|>>|\+=|-=|\*=|/=|%=|&=|\|=|\^=
        |[-+*/%<>=!&|^~?:.,()\[\]{}@\#;\\])
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class Token:
    kind: str  # number, ident, string, backtick, quote, op
    text: str


class LexError(ValueError):
    pass


def lex(formula: str) -> list[Token]:
    out: list[Token] = []
    position = 0
    while position < len(formula):
        match = _TOKEN.match(formula, position)
        if not match:
            raise LexError(f"cannot read {formula[position:position + 10]!r}")
        position = match.end()
        kind = match.lastgroup
        if kind != "space":
            out.append(Token(kind, match.group()))
    return out


@dataclass
class Builtins:
    functions: frozenset[str] = frozenset()
    constants: frozenset[str] = frozenset()

    @classmethod
    def load(cls, path: Path) -> "Builtins":
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return cls(frozenset(data["functions"]), frozenset(data["constants"]))


@dataclass
class Classification:
    subset: str  # "accepted" or "outside"
    tags: list[str]
    # Names that are neither columns nor default imports; a formula with any of these depends
    # on something outside the formula and its input table.
    unknown: list[str] = field(default_factory=list)


_IN_FILTER = re.compile(r"^\s*([A-Za-z_]\w*)\s+(?:icase\s+)?(?:not\s+)?(?:in|includes)\s", re.S)
_LITERAL = (
    r"(?:-?(?:0[xX][0-9a-fA-F_]+[lL]?|(?:\d[\d_]*\.?[\d_]*|\.\d[\d_]*)(?:[eE][+-]?\d+)?[fFdDlL]?)"
    r"|\"(?:\\.|[^\"\\])*\"|`[^`]*`|'(?:\\.|[^'\\])*'|true|false|null)"
)
_MATCH_FILTER = re.compile(rf"^\s*[A-Za-z_]\w*\s*(?:==|=|!=)\s*{_LITERAL}\s*$")
_RANGE_FILTER = re.compile(rf"^\s*[A-Za-z_]\w*\s*(?:<=|>=|<|>)\s*{_LITERAL}\s*$")

_MINIMAL_TAGS = {"arith", "compare", "logic", "string", "char", "null", "function:isNull"}
_OPERAND_START = {"number", "ident", "string", "backtick", "quote"}


def _is_char_literal(text: str) -> bool:
    body = text[1:-1]
    return len(body) == 1 or bool(re.fullmatch(r"\\(?:[btnfr0\"'\\]|u[0-9a-fA-F]{4})", body))


def classify(formula: str, kind: str, columns: dict[str, str], builtins: Builtins
             ) -> Classification:
    """Classifies `formula` (an expression for `update`, a filter for `where`).

    `columns` maps the columns of the input table to their fixture types.
    """
    tags: set[str] = set()
    outside: set[str] = set()
    unknown: list[str] = []

    def column(name: str) -> None:
        type_name = columns[name]
        if type_name == "?":  # a column made by an earlier formula; its type is not known here
            return
        tags.add("type:" + TYPE_TAGS.get(type_name, type_name))
        if type_name == "string":
            tags.add("string")
        if type_name == "char":
            tags.add("char")
        if type_name.endswith("[]"):
            tags.add("array")

    if kind == "where":
        in_filter = _IN_FILTER.match(formula)
        if in_filter and in_filter.group(1) in columns:
            column(in_filter.group(1))
            tags.update(("in", "filter:match"))
            if '"' in formula or "`" in formula:
                tags.add("string")
            if re.search(r"\bnull\b", formula):
                tags.update(("null", "null-literal"))
            return Classification("accepted", sorted(tags))
        if _MATCH_FILTER.match(formula):
            tags.add("filter:match")
        elif _RANGE_FILTER.match(formula):
            tags.add("filter:range")
        else:
            tags.add("filter:condition")

    try:
        tokens = lex(formula)
    except LexError:
        return Classification("outside", sorted(tags | {"unreadable"}))

    def text(index: int) -> str | None:
        return tokens[index].text if 0 <= index < len(tokens) else None

    def cast_at(index: int) -> bool:
        """True when the type name at `index` (possibly `a.b.C` or `T[]`) is `(type) operand`."""
        start = index
        while text(start - 1) == "." and start >= 2 and tokens[start - 2].kind == "ident":
            start -= 2
        end = index + 1
        while text(end) == "[" and text(end + 1) == "]":
            end += 2
        if text(start - 1) != "(" or text(end) != ")":
            return False
        # `(x) + y` must not read as a cast of `+y`; a cast is preceded by an operator or
        # nothing, and followed by the start of an operand.
        before = tokens[start - 2] if start >= 2 else None
        if before is not None and (before.kind in _OPERAND_START or before.text in (")", "]")):
            return False
        after = tokens[end + 1] if end + 1 < len(tokens) else None
        if after is None:
            return False
        return after.kind in _OPERAND_START or after.text in ("(", "-", "+", "!", "~")

    question_marks = 0
    for index, token in enumerate(tokens):
        previous, following = text(index - 1), text(index + 1)
        if token.kind == "number":
            continue
        if token.kind in ("string", "backtick"):
            tags.add("string")
            continue
        if token.kind == "quote":
            tags.add("char" if _is_char_literal(token.text) else "time-literal")
            continue
        if token.kind == "op":
            op = token.text
            if op in ("+", "-", "*", "/", "%"):
                tags.add("arith")
            elif op in ("==", "!=", "<", "<=", ">", ">="):
                tags.add("compare")
            elif op == "=" and kind == "where":
                tags.add("compare")
            elif op in ("&&", "||", "!"):
                tags.add("logic")
            elif op in ("&", "|", "^", "~"):
                tags.add("bitwise")
            elif op in ("<<", ">>", ">>>"):
                tags.add("shift")
            elif op == "?":
                question_marks += 1
                tags.add("ternary")
            elif op == "[":
                tags.add("array")
            elif op == "{":
                outside.add("array-literal")
            elif op in ("->", "::"):
                outside.add("lambda")
            elif op in ("=", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=", "<<=", ">>=",
                        ">>>=", "++", "--"):
                outside.add("assignment")
            continue

        name = token.text
        if previous == ".":
            if following == "(":
                outside.add("method-call")
                tags.add("method:" + name)
            else:
                outside.add("field-access")
            continue
        if name == "new":
            outside.add("new")
            continue
        if previous == "new":
            continue
        if name == "instanceof":
            outside.add("instanceof")
            continue
        if name in ("true", "false"):
            continue
        if name == "null":
            tags.update(("null", "null-literal"))
            continue
        if name in PRIMITIVES:
            if cast_at(index):
                tags.add("cast")
            else:
                outside.add("class-ref")
            continue
        if name in columns:
            column(name)
            continue
        if name.endswith("_") and name[:-1] in columns:
            column(name[:-1])
            tags.add("array")
            continue
        if name in ROW_VARS:
            tags.add("row-var")
            continue
        if following == "(":
            if name in builtins.functions:
                tags.add("function:" + name)
                if name == "isNull":
                    tags.add("null")
                if NONDETERMINISTIC.match(name):
                    tags.add("nondeterministic")
            else:
                outside.add("unknown-function")
                unknown.append(name)
            continue
        if name in builtins.constants:
            tags.add("constant")
            if name.startswith("NULL_"):
                tags.add("null")
            continue
        if cast_at(index) and following != ".":
            tags.add("cast")
            if name not in ACCEPTED_CAST_CLASSES or previous == ".":
                outside.add("cast-object")
            continue
        if following == "." or (name[:1].isupper() and following in ("[", "<")):
            outside.add("class-ref")
            continue
        outside.add("unknown-variable")
        unknown.append(name)

    if outside:
        return Classification("outside", sorted(tags | outside), unknown)
    plain = {t for t in tags if not t.startswith(("type:", "filter:"))}
    if plain <= _MINIMAL_TAGS and "null-literal" not in tags and "array" not in tags:
        tags.add("minimal")
    return Classification("accepted", sorted(tags), unknown)
