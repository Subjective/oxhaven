"""The systematic grid: every operator over every ordered pair of column types, and the rest.

No Java counterpart: this generates corpus cases; Java supplies the outcomes when recording.

The grid runs against three fixtures:

    pairs     81 rows: nine values per type, and every combination of a value on side 1
              (columns B1 S1 I1 L1 F1 D1 C1 Z1 T1) with a value on side 2 (B2 ... T2). The values
              include null, the extremes of the type next to its null sentinel, zero, NaN, the
              infinities, both zeros and the empty string.
    pairs_nz  the same, but zero is replaced by 3 in the integer columns of side 2, so that
              `%` can be recorded where the fixture above makes the whole column fail.
    one       a single row, for formulas made only of literals.

Column letters: B byte, S short, I int, L long, F float, D double, C char, Z Boolean, T String.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .. import scenario as sc
from .corpus import Fixture

INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1
FLOAT_MAX = 3.4028234663852886e38
DOUBLE_MAX = 1.7976931348623157e308

# (Java type, column letter, fixture type, the nine values)
TYPES = (
    ("byte", "B", "int8", [None, -127, -2, -1, 0, 1, 2, 7, 127]),
    ("short", "S", "int16", [None, -32767, -2, -1, 0, 1, 2, 7, 32767]),
    ("int", "I", "int32", [None, -INT32_MAX, -2, -1, 0, 1, 2, 7, INT32_MAX]),
    ("long", "L", "int64", [None, -INT64_MAX, -2, -1, 0, 1, 2, 7, INT64_MAX]),
    ("float", "F", "float32",
     [None, "NaN", "-Infinity", "Infinity", -0.0, 0.0, 1.5, -2.5, FLOAT_MAX]),
    ("double", "D", "float64",
     [None, "NaN", "-Infinity", "Infinity", -0.0, 0.0, 1.5, -2.5, DOUBLE_MAX]),
    ("char", "C", "char", [None, 0, 1, 2, 7, 48, 65, 97, 65534]),
    ("boolean", "Z", "bool", [None, False, True, None, False, True, None, False, True]),
    # "\U0001F600" is a surrogate pair and sorts below "\uFF5E" by UTF-16 code unit, above it
    # by code point.
    ("string", "T", "string", [None, "", "a", "A", "ab", "b", "10", "\U0001F600", "\uFF5E"]),
)
NUMERIC = ("byte", "short", "int", "long", "float", "double", "char")
INTEGRAL = ("byte", "short", "int", "long", "char")
LETTER = {name: letter for name, letter, _type, _values in TYPES}
ALL = tuple(name for name, _letter, _type, _values in TYPES)

BINARY = (
    ("plus", "+"), ("minus", "-"), ("times", "*"), ("divide", "/"), ("remainder", "%"),
    ("eq", "=="), ("ne", "!="), ("lt", "<"), ("le", "<="), ("gt", ">"), ("ge", ">="),
    ("and", "&&"), ("or", "||"), ("band", "&"), ("bor", "|"), ("xor", "^"),
)
COMPARE = tuple(pair for pair in BINARY if pair[0] in ("eq", "ne", "lt", "le", "gt", "ge"))

NULL_CONSTANT = {
    "byte": "NULL_BYTE", "short": "NULL_SHORT", "int": "NULL_INT", "long": "NULL_LONG",
    "float": "NULL_FLOAT", "double": "NULL_DOUBLE", "char": "NULL_CHAR",
    "boolean": "NULL_BOOLEAN",
}


@dataclass
class Draft:
    """A case before classification."""

    id: str
    kind: str
    fixture: str
    formula: str
    source: str = "grid"
    setup: list[tuple[str, list[str]]] = field(default_factory=list)
    # The columns of the table the case runs against, when `setup` changes them.
    columns: dict[str, str] | None = None


def _pairs(name: str, nonzero: bool) -> Fixture:
    columns = [sc.Column(f"{letter}{side}", type_name)
               for side in (1, 2) for _n, letter, type_name, _v in TYPES]
    rows = []
    for i in range(9):
        for j in range(9):
            row = [values[i] for _n, _l, _t, values in TYPES]
            for type_name, _l, _t, values in TYPES:
                cell = values[j]
                if nonzero and type_name in INTEGRAL and cell == 0:
                    cell = 3
                row.append(cell)
            rows.append(tuple(row))
    return Fixture(name, tuple(columns), tuple(rows), None)


def fixtures() -> dict[str, Fixture]:
    one = Fixture("one", (sc.Column("X", "int32"),), ((1,),), None)
    return {"pairs": _pairs("pairs", False), "pairs_nz": _pairs("pairs_nz", True), "one": one}


def _binary(out: list[Draft]) -> None:
    for name, op in BINARY:
        for a in ALL:
            for b in ALL:
                out.append(Draft(f"grid-binary/{name}.{a}.{b}", "update", "pairs",
                                 f"{LETTER[a]}1 {op} {LETTER[b]}2"))
    for a in INTEGRAL:
        for b in INTEGRAL:
            out.append(Draft(f"grid-binary/remainder-nonzero.{a}.{b}", "update", "pairs_nz",
                             f"{LETTER[a]}1 % {LETTER[b]}2"))


LITERALS = (
    ("int", "3"), ("long", "3L"), ("double", "1.5"), ("float", "1.5f"), ("char", "'a'"),
    ("boolean", "true"), ("string", '"a"'),
)

# Formulas made only of literals, run on the one-row fixture.
CONSTANT_EXPRESSIONS = (
    "1 + 1", "5 / 2", "-7 / 2", "5 % 2", "-7 % 3", "7 % -3", "1 / 0", "-1 / 0", "0 / 0", "1 % 0",
    "1L % 0L", "1.0 / 0", "1.0 % 0", "1.5 % 0.0", "5.5 % 2", "-5.5 % 2", "1 / 0.0f",
    "2147483647 + 1", "-2147483647 - 1", "-2147483647 - 2", "2147483647 * 2",
    "9223372036854775807L + 1", "-9223372036854775807L - 1", "-9223372036854775807L - 2",
    "2147483647 + 1L", "2147483648", "2147483648L", "-2147483648", "-9223372036854775808L",
    "9223372036854775808L", "0x7fffffff", "0xffffffff", "0x7fffffffffffffffL", "1_000", "0b101",
    "017", "1e3", "1e3f", "1d", "1f", ".5", "5.", "1e308 * 10", "-1e308 * 10", "1e-320",
    "3.4028235e38f * 2", "0.1 + 0.2", "0.1f + 0.2f", "0.1f + 0.2", "0.1f == 0.1", "1 == 1.0",
    "1L == 1", "16777217 == 16777217f", "9007199254740993L == 9007199254740993.0",
    "9007199254740993L + 0.0", "16777217 + 0f", "'a' + 1", "'a' + 'b'", "'a' * 2", "'a' / 2",
    "'a' == 97", "'a' < 'b'", "-'a'", "'\\n'", "'\\u0041'", "'\\''", "\"a\" + 1", "1 + \"a\"",
    "\"a\" + null", "null + \"a\"", "\"a\" + 'b'", "'a' + \"b\"", "\"a\" + 1.0", "\"a\" + 1.5f",
    "\"a\" + 1e20", "\"a\" + 1e-5", "\"a\" + 100.0f", "\"a\" + (1.0/0)", "\"a\" + (0.0/0)",
    "\"a\" + true", "\"a\" + 10000000000L", "1 + 2 + \"a\"", "\"a\" + 1 + 2", "\"a\" + (1 + 2)",
    "\"a\" + \"b\"", "`a` + `b`", "\"a\" == \"a\"", "\"a\" == `a`", "\"a\" != \"b\"",
    "\"a\" < \"b\"", "\"a\" < \"B\"", "\"a\" <= \"a\"", "\"\" < \"a\"", "\"a\" > null",
    "\"a\" == null", "\"a\\tb\\n\"", "\"\\u00e9\"", "\"a\" * 2", "\"a\" - \"a\"", "\"a\" && true",
    "true && false", "true || false", "!true", "true & false", "true | false", "true ^ true",
    "true == false", "true != false", "true < false", "true + 1", "true ? 1 : 2",
    "true ? 1 : 2.0", "true ? 1 : 2L", "false ? 1 : 'a'", "true ? 1 : \"a\"", "true ? 1 : null",
    "true ? null : 1", "true ? null : null", "false ? (byte)1 : (short)2", "true ? 1.5f : 2",
    "true ? 1.5f : 2L", "true ? 1.5f : 2.5", "true ? true : null", "null", "null == null",
    "null + 1", "null + null", "-null", "!null", "null && true", "null ? 1 : 2", "isNull(null)",
    "1 < 2 && 2 < 3", "1 < 2 < 3", "1 == 1 == true", "!(1 < 2)", "-(1 + 2)", "- -1", "+1",
    "~1", "~1L", "1 << 2", "8 >> 1", "-8 >>> 1", "1 & 3", "1 | 2", "1 ^ 3", "5 & 3L",
    "1.5 & 1", "(1 + 2) * 3", "1 + 2 * 3", "10 - 4 - 3", "2 * 3 % 4", "100 / 10 / 5",
    "1 + 2 == 3", "1 < 2 == true", "1 + 1.5f", "1L + 1.5f", "1L * 1.5", "1.5f * 1.5",
    "(byte)1 + (byte)2", "(short)1 * (short)2", "(byte)1 + 'a'", "(byte)1 / (byte)2",
    "(byte)127 + (byte)1", "(short)32767 + (short)1", "X", "X + 1", "X == 1", "X = 1",
)

# Casts of literals: narrowing, saturation, sentinels.
CONSTANT_CASTS = (
    "(int)3.7", "(int)-3.7", "(int)3.7f", "(long)3.7", "(long)-3.7", "(int)1e10", "(int)-1e10",
    "(long)1e19", "(long)-1e19", "(int)(0.0/0)", "(long)(0.0/0)", "(int)(1.0/0)",
    "(long)(-1.0/0)", "(byte)(0.0/0)", "(short)1e10", "(byte)1e10", "(char)1e10", "(byte)300",
    "(byte)128", "(byte)-128", "(byte)-129", "(byte)255", "(short)32768", "(short)-32768",
    "(short)65535", "(int)2147483648L", "(int)-2147483648L", "(int)4294967295L",
    "(int)9223372036854775807L", "(long)2147483647", "(char)65", "(char)-1", "(char)65535",
    "(char)65536", "(char)65.7", "(int)'a'", "(byte)'a'", "(long)'a'", "(double)'a'",
    "(float)1e40", "(float)-1e40", "(float)16777217", "(float)0.1", "(double)0.1f",
    "(double)16777217", "(float)9223372036854775807L", "(double)9223372036854775807L",
    "(long)9.223372036854776E18", "(int)(char)65", "(int)(byte)200", "(byte)(int)(char)200",
    "(double)(int)3.9", "(int)true", "(boolean)1", "(boolean)true", "(Boolean)true",
    "(String)\"a\"", "(String)1", "(int)\"1\"", "(int)null", "(double)null", "(String)null",
    "(Boolean)null", "(boolean)null", "(char)null", "(long)(int)null", "(int)NULL_LONG",
    "(long)NULL_INT", "(double)NULL_INT", "(int)NULL_DOUBLE", "(float)NULL_DOUBLE",
    "(double)NULL_FLOAT", "(byte)NULL_SHORT", "(short)NULL_BYTE", "(int)NULL_CHAR",
    "(char)NULL_INT", "(int) 1 + 2", "(double) 1 / 2", "(int) (1.5 + 2.5)", "(int) -1.5",
    "-(int) 1.5", "(double) (float) 1.1", "(long) 1.9f",
)


def _literals(out: list[Draft]) -> None:
    for name, op in BINARY:
        for a in ALL:
            out.append(Draft(f"grid-literal/{name}.{a}.null", "update", "pairs",
                             f"{LETTER[a]}1 {op} null"))
            out.append(Draft(f"grid-literal/{name}.null.{a}", "update", "pairs",
                             f"null {op} {LETTER[a]}1"))
    for name, op in BINARY:
        if name not in ("plus", "divide", "remainder", "eq", "lt"):
            continue
        for a in ALL:
            for literal_type, literal in LITERALS:
                out.append(Draft(f"grid-literal/{name}.{a}.literal-{literal_type}", "update",
                                 "pairs", f"{LETTER[a]}1 {op} {literal}"))
    zeros = (("int", "0"), ("long", "0L"), ("double", "0.0"), ("float", "0.0f"))
    for a in NUMERIC:
        for zero_type, zero in zeros:
            out.append(Draft(f"grid-literal/divide.{a}.zero-{zero_type}", "update", "pairs",
                             f"{LETTER[a]}1 / {zero}"))
            out.append(Draft(f"grid-literal/remainder.{a}.zero-{zero_type}", "update", "pairs",
                             f"{LETTER[a]}1 % {zero}"))
            out.append(Draft(f"grid-literal/divide.zero-{zero_type}.{a}", "update", "pairs",
                             f"{zero} / {LETTER[a]}1"))
    for number, formula in enumerate(CONSTANT_EXPRESSIONS):
        out.append(Draft(f"grid-literal/constant.{number:03d}", "update", "one", formula))


def _unary_and_casts(out: list[Draft]) -> None:
    for name, op in (("negate", "-"), ("plus", "+"), ("not", "!"), ("complement", "~")):
        for a in ALL:
            out.append(Draft(f"grid-unary/{name}.{a}", "update", "pairs", f"{op}{LETTER[a]}1"))
    targets = ("byte", "short", "int", "long", "float", "double", "char", "boolean", "Boolean",
               "String")
    for target in targets:
        for a in ALL:
            out.append(Draft(f"grid-cast/{target}.from-{a}", "update", "pairs",
                             f"({target}){LETTER[a]}1"))
    # Two casts in a row: what narrowing does to a value that has already been widened.
    for target in ("byte", "short", "int", "char"):
        for a in ("long", "float", "double"):
            out.append(Draft(f"grid-cast/{target}.via-long.from-{a}", "update", "pairs",
                             f"({target})(long){LETTER[a]}1"))
    for number, formula in enumerate(CONSTANT_CASTS):
        out.append(Draft(f"grid-cast/constant.{number:03d}", "update", "one", formula))


def _ternary(out: list[Draft]) -> None:
    for a in ALL:
        for b in ALL:
            out.append(Draft(f"grid-ternary/int-condition.{a}.{b}", "update", "pairs",
                             f"I1 > 0 ? {LETTER[a]}1 : {LETTER[b]}2"))
    for a in ALL:
        out.append(Draft(f"grid-ternary/then-null.{a}", "update", "pairs",
                         f"I1 > 0 ? null : {LETTER[a]}1"))
        out.append(Draft(f"grid-ternary/else-null.{a}", "update", "pairs",
                         f"I1 > 0 ? {LETTER[a]}1 : null"))
        out.append(Draft(f"grid-ternary/boolean-column-condition.{a}", "update", "pairs",
                         f"Z1 ? {LETTER[a]}1 : {LETTER[a]}2"))
        out.append(Draft(f"grid-ternary/null-test-condition.{a}", "update", "pairs",
                         f"isNull({LETTER[a]}1) ? {LETTER[a]}2 : {LETTER[a]}1"))
    extra = (
        "I1 > 0 ? (I2 > 0 ? 1 : 2) : 3", "I1 > 0 ? 1 : I2 > 0 ? 2 : 3", "(I1 > 0 ? 1 : 2) + 1",
        "I1 > 0 ? I1 + 1 : D1 * 2", "I1 > 0 && I2 > 0 ? I1 : I2", "isNull(I1) ? 0 : I1",
        "isNull(I1) ? NULL_INT : I1 + 1", "I1 == null ? 0 : I1", "I1 > I2 ? I1 : I2",
        "D1 > D2 ? D1 : D2", "Z1 == true ? 1 : 0", "Z1 != null && Z1 ? 1 : 0",
        "isNull(Z1) ? false : Z1", "I1 > 0 ? \"pos\" : \"non-pos\"", "I1 > 0 ? T1 : \"none\"",
        "I1 > 0 ? 'a' : 'b'", "I1 > 0 ? true : false", "I1 > 0 ? 1 : 1.5", "I1 > 0 ? 1 : 1L",
        "I1 > 0 ? (byte)1 : (short)2", "I1 > 0 ? B1 : 1", "I1 > 0 ? C1 : 1", "I1 > 0 ? 1.5f : 2L",
    )
    for number, formula in enumerate(extra):
        out.append(Draft(f"grid-ternary/extra.{number:02d}", "update", "pairs", formula))


def _nulls(out: list[Draft]) -> None:
    for a in ALL:
        out.append(Draft(f"grid-null/isNull.{a}", "update", "pairs", f"isNull({LETTER[a]}1)"))
        out.append(Draft(f"grid-null/not-isNull.{a}", "update", "pairs",
                         f"!isNull({LETTER[a]}1)"))
    for a, constant in NULL_CONSTANT.items():
        letter = LETTER[a]
        out.append(Draft(f"grid-null/constant.{a}", "update", "one", constant))
        out.append(Draft(f"grid-null/constant-isNull.{a}", "update", "one", f"isNull({constant})"))
        for name, op in (("eq", "=="), ("ne", "!="), ("lt", "<"), ("ge", ">="), ("plus", "+")):
            out.append(Draft(f"grid-null/{name}-constant.{a}", "update", "pairs",
                             f"{letter}1 {op} {constant}"))
    for a in NUMERIC:
        letter = LETTER[a]
        out.append(Draft(f"grid-null/isNull-sum.{a}", "update", "pairs",
                         f"isNull({letter}1 + {letter}2)"))
        # Arithmetic that lands on the null sentinel of the result type.
        out.append(Draft(f"grid-null/minus-one.{a}", "update", "pairs", f"{letter}1 - 1"))
        out.append(Draft(f"grid-null/plus-one.{a}", "update", "pairs", f"{letter}1 + 1"))
        out.append(Draft(f"grid-null/times-minus-one.{a}", "update", "pairs", f"{letter}1 * -1"))
        out.append(Draft(f"grid-null/narrowed-minus-one.{a}", "update", "pairs",
                         f"({a})({letter}1 - 1)"))
    extra = (
        "NULL_INT == NULL_LONG", "NULL_INT == NULL_DOUBLE", "NULL_BYTE == NULL_INT",
        "NULL_INT + 1", "-NULL_INT", "NULL_LONG - 1", "NULL_DOUBLE * 2", "-NULL_DOUBLE",
        "NULL_FLOAT + 1", "NULL_INT < 0", "NULL_INT > 0", "NULL_INT <= NULL_INT",
        "NULL_DOUBLE < NAN_DOUBLE", "NAN_DOUBLE == NAN_DOUBLE", "NAN_DOUBLE != NAN_DOUBLE",
        "NAN_DOUBLE < 1", "NAN_DOUBLE > 1", "NAN_DOUBLE + 1", "NAN_FLOAT == NAN_DOUBLE",
        "POS_INFINITY_DOUBLE - POS_INFINITY_DOUBLE", "POS_INFINITY_DOUBLE > MAX_DOUBLE",
        "NEG_INFINITY_DOUBLE < NULL_DOUBLE", "NEG_INFINITY_DOUBLE == NULL_DOUBLE",
        "MAX_INT + 1", "MIN_INT - 1", "MIN_INT", "MAX_LONG + 1", "MIN_LONG - 1", "MAX_BYTE + 1",
        "MAX_CHAR", "MIN_CHAR", "MAX_FLOAT * 2", "MIN_FLOAT", "MIN_DOUBLE", "-MAX_DOUBLE",
        "-MAX_FLOAT", "MIN_POS_DOUBLE / 2", "MAX_FINITE_DOUBLE", "-2147483648 == NULL_INT",
        "isNull(-2147483647 - 1)", "isNull(-1.7976931348623157E308)",
        "isNull(-3.4028235E38f)", "isNull((char)65535)", "isNull((byte)-128)", "isNull(\"\")",
        "isNull(1 / 0)", "isNull(0.0 / 0)", "isNull(NAN_DOUBLE)",
    )
    for number, formula in enumerate(extra):
        out.append(Draft(f"grid-null/extra.{number:02d}", "update", "one", formula))


def _functions(out: list[Draft]) -> None:
    one = ("abs", "signum", "sqrt", "floor", "ceil", "round", "rint", "exp", "log", "sin",
           "isNaN", "isInf", "isFinite")
    for function in one:
        for a in NUMERIC:
            out.append(Draft(f"grid-function/{function}.{a}", "update", "pairs",
                             f"{function}({LETTER[a]}1)"))
    for function in ("min", "max", "pow"):
        for a in NUMERIC:
            for b in NUMERIC:
                out.append(Draft(f"grid-function/{function}.{a}.{b}", "update", "pairs",
                                 f"{function}({LETTER[a]}1, {LETTER[b]}2)"))
    for a in ALL:
        letter = LETTER[a]
        out.append(Draft(f"grid-function/replaceIfNull.{a}", "update", "pairs",
                         f"replaceIfNull({letter}1, {letter}2)"))
        out.append(Draft(f"grid-function/in.{a}", "update", "pairs",
                         f"in({letter}1, {letter}2)"))
        out.append(Draft(f"grid-function/inRange.{a}", "update", "pairs",
                         f"inRange({letter}1, {letter}2, {letter}1)"))
    for a in NUMERIC:
        letter = LETTER[a]
        out.append(Draft(f"grid-function/clamp.{a}", "update", "pairs",
                         f"clamp({letter}1, {letter}2, {letter}2)"))
    # The operators and casts by their function names, as the Java parser emits them.
    explicit = (
        "plus(I1, I2)", "minus(I1, L2)", "multiply(F1, I2)", "divide(I1, I2)",
        "remainder(I1, 3)", "eq(I1, I2)", "less(I1, I2)", "lessEquals(D1, D2)",
        "greater(I1, D2)", "greaterEquals(L1, I2)", "compareTo(I1, I2)", "compareTo(D1, D2)",
        "negate(I1)", "not(Z1)", "binaryAnd(Z1, Z2)", "binaryOr(Z1, Z2)", "xor(Z1, Z2)",
        "intCast(D1)", "doubleCast(I1)", "longCast(F1)", "byteCast(I1)", "charCast(I1)",
        "shortCast(L1)", "floatCast(D1)",
    )
    for number, formula in enumerate(explicit):
        out.append(Draft(f"grid-function/operator-name.{number:02d}", "update", "pairs", formula))
    # What users write on strings is Java: methods of java.lang.String. Outside the subset.
    methods = (
        "T1.length()", "T1.toUpperCase()", "T1.toLowerCase()", "T1.trim()", "T1.isEmpty()",
        "T1.equals(T2)", "T1.equalsIgnoreCase(T2)", "T1.compareTo(T2)", "T1.contains(\"a\")",
        "T1.startsWith(\"a\")", "T1.endsWith(\"b\")", "T1.substring(0, 1)", "T1.charAt(0)",
        "T1.indexOf(\"b\")", "T1.replace(\"a\", \"x\")", "T1.concat(T2)", "T1.hashCode()",
        "T1 + T2.length()", "String.valueOf(I1)", "Integer.parseInt(\"12\")",
        "Math.abs(I1)", "Math.max(I1, I2)", "Math.sqrt(D1)", "Math.floorMod(I1, 3)",
        "Integer.MAX_VALUE", "Double.NaN", "Double.isNaN(D1)", "Long.MIN_VALUE",
        "new String(\"a\")", "new Integer(I1)", "I1 instanceof Object",
    )
    for number, formula in enumerate(methods):
        out.append(Draft(f"grid-function/java.{number:02d}", "update", "pairs", formula))
        if formula.startswith("T1"):
            # A null receiver makes the whole column fail; this records the non-null rows.
            guard = "isNull(T1) || isNull(T2)" if "T2" in formula else "isNull(T1)"
            out.append(Draft(f"grid-function/java-guarded.{number:02d}", "update", "pairs",
                             f"{guard} ? null : {formula}"))


def _arrays(out: list[Draft]) -> None:
    for a in ALL:
        letter = LETTER[a]
        out.append(Draft(f"grid-array/previous.{a}", "update", "pairs", f"{letter}1_[i - 1]"))
        out.append(Draft(f"grid-array/next.{a}", "update", "pairs", f"{letter}2_[i + 1]"))
    extra = (
        "i", "ii", "k", "i + 1", "ii * 2", "i + ii", "i % 2 == 0", "i / 2", "(double)i",
        "i == ii", "k - ii", "I1_[i]", "I1_[ii]", "I2_[i - 100]", "I2_[i + 100]", "I2_[-1]",
        "I2_[0]", "I2_[80]", "I2_[81]", "I2_[i - 1] + I2", "I2 - I2_[i - 1]",
        "(I2_[i - 1] + I2_[i + 1]) / 2", "D2_[i - 1] / D2", "isNull(I2_[i - 1])",
        "I2_[i - 1] == null ? 0 : I2_[i - 1]", "I2_[(int)L2 % 3]", "I2_[i - I2]",
        "I2_.size()", "I2_.get(i)", "I2_.subVector(0, 2)", "len(I2_)", "sum(I2_)", "avg(D2_)",
        "min(I2_)", "max(D2_)", "first(I2_)", "last(I2_)", "count(I2_)", "countDistinct(I2_)",
        "I2_", "T2_[i - 1]", "T2_[i - 1] + T2", "Z2_[i - 1] && Z2", "C2_[i - 1] + 1",
    )
    for number, formula in enumerate(extra):
        out.append(Draft(f"grid-array/extra.{number:02d}", "update", "pairs", formula))


WHERE_LITERALS = {
    "byte": ("null", "0", "1", "-1", "7", "127", "128", "1.5"),
    "short": ("null", "0", "1", "-1", "7", "32767"),
    "int": ("null", "0", "1", "-1", "7", "2147483647", "2147483648", "7L", "1.0", "1.5",
            "NULL_INT", "'a'"),
    "long": ("null", "0", "1", "-1", "7", "9223372036854775807", "7L", "1.5"),
    "float": ("null", "0", "0.0", "-0.0", "1.5", "1.5f", "-2.5", "NaN", "NAN_FLOAT",
              "POS_INFINITY_FLOAT", "3.4028235E38"),
    "double": ("null", "0", "0.0", "-0.0", "1.5", "-2.5", "NaN", "NAN_DOUBLE", "Infinity",
               "POS_INFINITY_DOUBLE", "NEG_INFINITY_DOUBLE", "1.7976931348623157E308"),
    "char": ("null", "'A'", "'a'", "'0'", "65"),
    "boolean": ("null", "true", "false"),
    "string": ("null", "\"a\"", "`a`", "\"\"", "\"A\"", "\"ab\"", "'a'", "a"),
}


def _where(out: list[Draft]) -> None:
    for name, op in COMPARE:
        for a in ALL:
            for b in ALL:
                out.append(Draft(f"grid-where/columns.{name}.{a}.{b}", "where", "pairs",
                                 f"{LETTER[a]}1 {op} {LETTER[b]}2"))
    for a in ALL:
        for number, literal in enumerate(WHERE_LITERALS[a]):
            for name, op in COMPARE + (("assign", "="),):
                out.append(Draft(f"grid-where/literal.{name}.{a}.{number:02d}", "where", "pairs",
                                 f"{LETTER[a]}1 {op} {literal}"))
            if number < 4:
                for name, op in (("eq", "=="), ("lt", "<"), ("ge", ">=")):
                    out.append(Draft(f"grid-where/literal-first.{name}.{a}.{number:02d}",
                                     "where", "pairs", f"{literal} {op} {LETTER[a]}1"))
    for a in ALL:
        letter = LETTER[a]
        out.append(Draft(f"grid-where/isNull.{a}", "where", "pairs", f"isNull({letter}1)"))
        out.append(Draft(f"grid-where/not-isNull.{a}", "where", "pairs", f"!isNull({letter}1)"))
        out.append(Draft(f"grid-where/same-column.eq.{a}", "where", "pairs",
                         f"{letter}1 == {letter}1"))
        out.append(Draft(f"grid-where/same-column.lt.{a}", "where", "pairs",
                         f"{letter}1 < {letter}1"))
        out.append(Draft(f"grid-where/same-column.le.{a}", "where", "pairs",
                         f"{letter}1 <= {letter}1"))
    for a in NUMERIC:
        letter = LETTER[a]
        for b in NUMERIC:
            out.append(Draft(f"grid-where/sum-positive.{a}.{b}", "where", "pairs",
                             f"{letter}1 + {LETTER[b]}2 > 0"))
        out.append(Draft(f"grid-where/even.{a}", "where", "pairs", f"{letter}1 % 2 == 0"))
        out.append(Draft(f"grid-where/half-positive.{a}", "where", "pairs", f"{letter}1 / 2 > 0"))
        out.append(Draft(f"grid-where/double-equals.{a}", "where", "pairs",
                         f"{letter}1 * 2 == {letter}2"))
        out.append(Draft(f"grid-where/between.{a}", "where", "pairs",
                         f"{letter}1 > 0 && {letter}1 < 7"))
        out.append(Draft(f"grid-where/outside.{a}", "where", "pairs",
                         f"{letter}1 <= 0 || {letter}1 >= 7"))
        out.append(Draft(f"grid-where/negated.{a}", "where", "pairs", f"!({letter}1 > 0)"))
        out.append(Draft(f"grid-where/inRange.{a}", "where", "pairs",
                         f"inRange({letter}1, {letter}2, {letter}2)"))
    extra = (
        "Z1", "!Z1", "Z1 && Z2", "Z1 || Z2", "Z1 & Z2", "Z1 | Z2", "Z1 ^ Z2", "Z1 == Z2",
        "Z1 != Z2", "Z1 == true", "Z1 != true", "Z1 == false", "isNull(Z1) || Z1",
        "!isNull(Z1) && Z1", "Z1 != null && Z1", "Z1 == null || !Z1", "true", "false", "null",
        "1 == 1", "1 > 2", "I1", "I1 + 1", "T1", "I1 > 0 && I2 > 0", "I1 > 0 || I2 > 0",
        "I1 > 0 && D1 > 0", "isNull(I1) || I1 > 0", "!isNull(I1) && I1 > 0",
        "I1 != null && I1 > 0", "I1 == null || I1 > 0", "I1 > 0 ? true : false",
        "I1 > 0 ? Z1 : Z2", "(I1 > 0) == (I2 > 0)", "I1 == I2 && D1 == D2", "I1 > I2 == true",
        "I1 in 1, 2, 7", "I1 not in 1, 2, 7", "I1 in 1", "I1 in null", "I1 in null, 1",
        "I1 not in null", "I1 in 1L, 2L", "I1 in 1.0, 2.0", "I1 in 1.5", "I1 in 'a'",
        "I1 in 2147483648", "I1 in I2", "I1 in 1, I2", "L1 in 1, 2, 9223372036854775807",
        "B1 in 1, 127", "B1 in 128", "S1 in 7, 32767", "C1 in 'A', 'a'", "C1 in 65",
        "C1 not in 'A'", "C1 in null", "F1 in 1.5, -2.5", "F1 in NaN", "F1 in null", "F1 in 0",
        "F1 in -0.0", "D1 in 1.5, -2.5", "D1 in NaN", "D1 not in NaN", "D1 in null",
        "D1 in 0.0", "D1 in -0.0", "D1 in Infinity", "D1 in 1", "Z1 in true", "Z1 in false",
        "Z1 in true, false", "Z1 in null", "Z1 not in true", "T1 in \"a\"", "T1 in `a`, `A`",
        "T1 in \"a\", \"ab\"", "T1 not in \"a\"", "T1 in null", "T1 in \"\"", "T1 in a, ab",
        "T1 in 'a'", "T1 icase in \"a\"", "T1 icase in `AB`", "T1 icase not in \"a\"",
        "T1 includes any \"a\"", "T1 includes all \"a\", \"b\"", "T1 not includes any \"a\"",
        "T1.startsWith(\"a\")", "T1 != null && T1.length() > 1", "T1.length() > 1",
        "T1 == T2 && T1 != null", "T1 + T2 == \"aa\"", "T1 == \"a\" || T1 == \"A\"",
        "I1 = 1", "I1 = I2", "I1 == 1 && I2 == 1", "I1 > 0, I2 > 0", "I1 >", "I1 > Nope",
        "I1 === 1", "I1 <> 1", "I1 > 0 and I2 > 0", "I1_[i - 1] < I1", "i > 40", "ii % 2 == 0",
        "k < 10", "i < 5 || i > 75",
    )
    for number, formula in enumerate(extra):
        out.append(Draft(f"grid-where/extra.{number:03d}", "where", "pairs", formula))


def drafts() -> list[Draft]:
    out: list[Draft] = []
    _binary(out)
    _literals(out)
    _unary_and_casts(out)
    _ternary(out)
    _nulls(out)
    _functions(out)
    _arrays(out)
    _where(out)
    return out
