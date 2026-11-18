//! Tests of the Java arrays of the `jpy` module the binary holds (`python/jpy.py`): what
//! `jpy.array`, `jpy.cast` and `jpy.convert` make and answer, `deephaven.dtypes.array`,
//! and an array in a formula.
//!
//! Every expectation is what jpy 2.2.0 answered to the same code inside the Deephaven 42.9
//! server, unless a comment says how the reference differs.

use arrow::array::AsArray;

use super::column_tests::{described, error};
use super::tests::Python;

const SETUP: &str = r#"
import jpy

def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e)
    return None

def stored(name, value):
    made = jpy.array(name, 1)
    made[0] = value
    return made[0]
"#;

/// An array of a primitive type: its class, its positions, its buffer, and how it is no
/// Python sequence.
#[tokio::test]
async fn a_primitive_array_has_the_class_positions_and_buffer_of_jpys() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
a = jpy.array('long', [1, 2, 3])
cls = type(a)
assert cls is jpy.get_type('[J') and cls.jclassname == '[J'
assert (cls.__name__, cls.__qualname__, cls.__module__) == ('[J', '[J', 'builtins')
assert cls.__mro__[1:3] == (jpy.get_type('java.lang.Object'), jpy.JType), cls.__mro__
assert isinstance(a, jpy.JType) and isinstance(a, jpy.get_type('java.lang.Object'))
assert not isinstance(a, jpy.get_type('[I')) and issubclass(cls, jpy.get_type('java.lang.Object'))
assert jpy.array(jpy.get_type('long'), [1]).__class__ is cls

assert len(a) == 3 and (a[0], a[-1], a[True]) == (1, 3, 2) and list(a) == [1, 2, 3]
assert list(reversed(a)) == [3, 2, 1] and sorted(a) == [1, 2, 3] and 2 in a and 'x' not in a
assert raised(lambda: a[3]) == ('IndexError', 'Java array index out of bounds')
assert raised(lambda: a[-4]) == ('IndexError', 'Java array index out of bounds')
assert raised(lambda: a['0']) == ('TypeError', "sequence index must be integer, not 'str'")
assert raised(lambda: a[1.0]) == ('TypeError', "sequence index must be integer, not 'float'")
assert raised(lambda: a[0:2]) == ('TypeError', "sequence index must be integer, not 'slice'")
assert raised(lambda: a[2**70]) == ('IndexError', "cannot fit 'int' into an index-sized integer")

a[1] = 7
a[-1] = 9
assert list(a) == [1, 7, 9]
assert raised(lambda: a.__setitem__(3, 1)) == ('RuntimeError', 'java.lang.ArrayIndexOutOfBoundsException: Array region 3..4 out of bounds for length 3')
assert raised(lambda: a.__setitem__(-4, 1)) == ('RuntimeError', 'java.lang.ArrayIndexOutOfBoundsException: Array region -1..0 out of bounds for length 3')
assert raised(lambda: a.__delitem__(0)) == ('RuntimeError', 'cannot delete items of Java arrays')

view = memoryview(a)
assert (view.format, view.itemsize, view.shape) == ('q', 8, (3,))
assert bytes(view) == b'\x01\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00'
for name, letter, fmt, size in (('boolean', 'Z', 'B', 1), ('byte', 'B', 'b', 1), ('short', 'S', 'h', 2),
                                ('char', 'C', 'H', 2), ('int', 'I', 'i', 4), ('float', 'F', 'f', 4),
                                ('double', 'D', 'd', 8)):
    made = jpy.array(name, 2)
    assert type(made).__name__ == '[' + letter
    assert (memoryview(made).format, memoryview(made).itemsize) == (fmt, size), name
    assert list(made) == ([False, False] if name == 'boolean' else [0, 0]), name

# what a Python sequence has and a Java array has not
assert raised(lambda: a.append(1)) == ('AttributeError', "'[J' object has no attribute 'append'")
assert raised(lambda: a.length) == ('AttributeError', "'[J' object has no attribute 'length'")
assert raised(lambda: a + a) == ('TypeError', "unsupported operand type(s) for +: '[J' and '[J'")
assert raised(lambda: a * 2) == ('TypeError', "unsupported operand type(s) for *: '[J' and 'int'")
import copy, pickle
assert raised(lambda: copy.copy(a)) == ('TypeError', "cannot pickle '[J' object")
assert raised(lambda: pickle.dumps(a)) == ('TypeError', "cannot pickle '[J' object")
assert [n for n in dir(a) if not n.startswith('__')] == ['equals', 'getClass', 'hashCode', 'jclass', 'jclassname', 'notify', 'notifyAll', 'toString', 'wait']

# a Java object: equal to itself only, ordered against Java objects only
b = jpy.array('long', [1, 7, 9])
assert a == a and a != b and not a == b and not a == [1, 7, 9]
assert not a != 1 and not a < 1 and not a <= 1 and not a > 1 and not a >= 1
assert not a < a and a <= a and not a > a and a >= a and (a < b) != (a > b)
assert hash(a) == hash(a) == a.hashCode() and a.equals(a) and not a.equals(b) and not a.equals(None)
assert bool(a) and not bool(jpy.array('long', 0))

# the texts have the shape of jpy's; the numbers are Java's references there
import re
assert re.fullmatch(r'\[J@[0-9a-f]+', str(a)) and a.toString() == str(a), str(a)
assert re.fullmatch(r'\[J\(objectRef=0x[0-9a-f]+\)', repr(a)), repr(a)
"#,
    );
}

/// What each primitive type makes of a Python value, where an array is made and where an
/// element is set: jpy sets -1 where it cannot convert, and fails where it makes.
#[tokio::test]
async fn a_primitive_array_converts_values_as_jpy_does() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
int_too_big = ('OverflowError', 'Python int too large to convert to C long')
cases = {
    'boolean': {1: True, -1: True, 1.5: True, None: False, 'a': True, 2**63: True},
    'byte': {1: 1, -1: -1, 300: 44, 70000: 112, 2**31: 0, 2**32 + 5: 5, True: 1, None: 0},
    'short': {300: 300, 70000: 4464, 2**31: 0, 2**32 + 5: 5},
    'char': {-1: 65535, 300: 300, 70000: 4464},
    'int': {70000: 70000, 2**31: -2**31, 2**32 + 5: 5},
    'long': {-(2**63): -(2**63), 2**32 + 5: 2**32 + 5},
    'float': {1: 1.0, 2**32 + 5: 4294967296.0, 1.5: 1.5, 0.1: 0.10000000149011612, 1e300: float('inf'), True: 1.0, None: 0.0, 2**63: 9.223372036854776e18},
    'double': {2**32 + 5: 4294967301.0, 1e300: 1e300, None: 0.0},
}
for name, values in cases.items():
    for value, expected in values.items():
        got = jpy.array(name, [value])[0]
        assert got == expected and type(got) is type(expected), (name, value, got)
assert str(jpy.array('double', [float('nan')])[0]) == 'nan'

for name in ('byte', 'short', 'char', 'int', 'long'):
    assert raised(lambda: jpy.array(name, [1.5])) == ('TypeError', "'float' object cannot be interpreted as an integer")
    assert raised(lambda: jpy.array(name, ['a'])) == ('TypeError', "'str' object cannot be interpreted as an integer")
    assert raised(lambda: jpy.array(name, [b'a'])) == ('TypeError', "'bytes' object cannot be interpreted as an integer")
    if name == 'long':
        assert raised(lambda: jpy.array(name, [2**63])) == ('OverflowError', 'int too big to convert')
    else:
        assert raised(lambda: jpy.array(name, [2**63])) == int_too_big, name
        assert raised(lambda: jpy.array(name, [-(2**63) - 1])) == int_too_big, name
for name in ('float', 'double'):
    assert raised(lambda: jpy.array(name, ['a'])) == ('TypeError', 'must be real number, not str')
    assert raised(lambda: jpy.array(name, [b'a'])) == ('TypeError', 'must be real number, not bytes')
assert raised(lambda: jpy.array('double', [2**1100])) == ('OverflowError', 'int too large to convert to float')

# setting an element: what cannot be converted is -1, and nothing fails
for name, value, expected in (('long', 'x', -1), ('long', 1.5, -1), ('long', 2**63, -1), ('long', None, 0),
                              ('long', True, 1), ('byte', 300, 44), ('byte', 'a', -1), ('short', 1.5, -1),
                              ('char', 1.5, 65535), ('char', -1, 65535), ('int', 'a', -1), ('int', 2**63, -1),
                              ('float', 'a', -1.0), ('float', 1e300, float('inf')), ('double', 'a', -1.0),
                              ('double', None, 0.0), ('boolean', None, False), ('boolean', 'a', True)):
    got = stored(name, value)
    assert got == expected and type(got) is type(expected), (name, value, got)

# the initializer: a length or a sequence, read by position
assert list(jpy.array('long', 3)) == [0, 0, 0] and list(jpy.array('long', True)) == [0]
assert list(jpy.array('long', (1, 2))) == [1, 2] and list(jpy.array('long', range(3))) == [0, 1, 2]
assert list(jpy.array('long', b'ab')) == [97, 98]
not_init = ('ValueError', 'array: argument 2 (init) must be either an integer array length or any sequence')
for init in (-1, 2.0, None, (x for x in (1, 2)), {1: 2}, {1}, 2**31):
    assert raised(lambda: jpy.array('long', init)) == not_init, init
assert raised(lambda: jpy.array('long', 'abc')) == ('TypeError', "'str' object cannot be interpreted as an integer")
"#,
    );
}

/// An array of a class: the values each class takes, what it gives back, and its texts
/// for a value it cannot take.
#[tokio::test]
async fn an_array_of_a_class_converts_values_as_jpy_does() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
s = jpy.array('java.lang.String', ['a', None, 'c'])
cls = type(s)
assert cls is jpy.get_type('[Ljava.lang.String;') and (cls.__name__, cls.__module__) == ('String;', '[Ljava.lang')
assert jpy.array('java/lang/String', 1).__class__ is cls and jpy.array(jpy.get_type('java.lang.String'), 1).__class__ is cls
assert list(s) == ['a', None, 'c'] and 'a' in s and list(jpy.array('java.lang.String', 2)) == [None, None]
assert list(jpy.array('java.lang.String', 'abc')) == ['a', 'b', 'c']
# jpy's text names the class '[Ljava.lang.String;'; Python names the type 'String;' here
assert raised(lambda: memoryview(s))[0] == 'TypeError'
s[0] = 'x'
s[1] = None
assert list(s) == ['x', None, 'c']
assert raised(lambda: s.__setitem__(-4, 'x')) == ('RuntimeError', 'java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 3')
assert raised(lambda: s.__setitem__(5, 'x')) == ('RuntimeError', 'java.lang.ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 3')
assert raised(lambda: s.__setitem__(5, 1)) == ('ValueError', "cannot convert a Python 'int' to a Java 'java.lang.String'")
assert raised(lambda: s.__delitem__(0)) == ('RuntimeError', 'cannot delete items of Java arrays')

def refused(name, value):
    return ('ValueError', f"cannot convert a Python '{value}' to a Java '{name}'")

for value, words in ((1, 'int'), (1.5, 'float'), (True, 'bool'), (b'a', 'bytes'), (object(), 'object')):
    assert raised(lambda: jpy.array('java.lang.String', [value])) == refused('java.lang.String', words)
    assert raised(lambda: stored('java.lang.String', value)) == refused('java.lang.String', words)
assert jpy.array('java.lang.String', [type('S', (str,), {})('q')])[0].__class__ is str

def kinds(name, values):
    return [(type(x).__name__, x) for x in jpy.array(name, values)]

assert kinds('java.lang.Object', [1, 'a', 1.5, True, None, 2**40]) == [
    ('int', 1), ('str', 'a'), ('float', 1.5), ('bool', True), ('NoneType', None), ('int', 2**40)]
for value, words in (([1], 'list'), ((1,), 'tuple'), ({1: 2}, 'dict'), (b'x', 'bytes'), (object(), 'object')):
    assert raised(lambda: jpy.array('java.lang.Object', [value])) == refused('java.lang.Object', words)
assert raised(lambda: stored('java.lang.Object', [1])) == refused('java.lang.Object', 'list')
assert raised(lambda: jpy.array('java.lang.Object', [2**63]))[0] == 'SystemError'
assert raised(lambda: stored('java.lang.Object', 2**64))[0] == 'SystemError'
assert kinds('java.lang.Integer', [1, None, 2**40, True, -1, 2**31]) == [
    ('int', 1), ('NoneType', None), ('int', 0), ('int', 1), ('int', -1), ('int', -(2**31))]
assert kinds('java.lang.Long', [2**40]) == [('int', 2**40)]
assert kinds('java.lang.Short', [40000]) == [('int', -25536)] and kinds('java.lang.Byte', [200]) == [('int', -56)]
assert kinds('java.lang.Character', [-1, 65]) == [('int', 65535), ('int', 65)]
assert kinds('java.lang.Double', [1, 1.5, True]) == [('float', 1.0), ('float', 1.5), ('float', 1.0)]
assert kinds('java.lang.Float', [0.1, 1e300]) == [('float', 0.10000000149011612), ('float', float('inf'))]
assert kinds('java.lang.Boolean', [1, None, 'a', [1], 1.5]) == [
    ('bool', True), ('NoneType', None), ('bool', True), ('bool', True), ('bool', True)]
assert raised(lambda: jpy.array('java.lang.Long', [2**63]))[0] == 'SystemError'
assert raised(lambda: stored('java.lang.Long', 2**64))[0] == 'SystemError'
for name in ('java.lang.Integer', 'java.lang.Long', 'java.lang.Short', 'java.lang.Byte', 'java.lang.Character'):
    assert raised(lambda: jpy.array(name, [1.5])) == refused(name, 'float')
    assert raised(lambda: jpy.array(name, ['a'])) == refused(name, 'str')
assert raised(lambda: stored('java.lang.Integer', 1.5)) == refused('java.lang.Integer', 'float')
assert raised(lambda: jpy.array('java.lang.Double', ['a'])) == refused('java.lang.Double', 'str')
held = [1]
assert jpy.array('org.jpy.PyObject', [held])[0] is held and stored('org.jpy.PyObject', held) is held

# the values of `java.time`, which the package has
from deephaven.time import to_j_instant, to_j_local_date, to_j_time_zone
instant = to_j_instant('2024-01-01T00:00:00Z')
instants = jpy.array('java.time.Instant', [instant, None])
assert type(instants).__name__ == 'Instant;' and [str(x) for x in instants] == ['2024-01-01T00:00:00Z', 'None']
assert instants[0] == instant
date = to_j_local_date('2024-01-01')
assert [str(x) for x in jpy.array('java.time.LocalDate', [date])] == ['2024-01-01']
assert raised(lambda: jpy.array('java.time.Instant', [date])) == refused('java.time.Instant', 'java.time.LocalDate')
zone = to_j_time_zone('America/New_York')
assert raised(lambda: jpy.array('java.time.Instant', [zone])) == refused('java.time.Instant', 'java.time.ZoneId')
for value, words in ((1, 'int'), ('a', 'str'), (True, 'bool')):
    assert raised(lambda: jpy.array('java.time.Instant', [value])) == refused('java.time.Instant', words)
assert jpy.array('java.lang.Object', [instant])[0] == instant

# arrays of arrays: an array of the element class is kept, and so shared, a sequence is a
# new array of it
inner = jpy.array('long', [1])
outer = jpy.array('[J', [inner, None, [2, 3], (4,)])
assert type(outer).__name__ == '[[J' and outer[1] is None and [list(x) for x in (outer[2], outer[3])] == [[2, 3], [4]]
inner[0] = 9
assert list(outer[0]) == [9]
assert [list(x) for x in jpy.array('[J', [jpy.array('int', [4])])] == [[4]]
assert list(jpy.array('[J', 2)) == [None, None]
assert [list(x) for x in jpy.array('[Ljava.lang.String;', [['a'], 'ab'])] == [['a'], ['a', 'b']]
assert raised(lambda: jpy.array('[J', [3])) == ('ValueError', "cannot convert a Python 'int' to a Java array of type 'long'")
assert raised(lambda: jpy.array('[Ljava.lang.String;', [3])) == ('ValueError', "cannot convert a Python 'int' to a Java array of type 'java.lang.String'")
assert raised(lambda: jpy.array('[J', ['ab'])) == ('TypeError', "'str' object cannot be interpreted as an integer")
held = jpy.array('long', [5])
assert jpy.array('java.lang.Object', [held])[0] is held
assert type(jpy.array('[Ljava.lang.String;', 1)).jclassname == '[[Ljava.lang.String;'

# names no class has, and what is no type
assert raised(lambda: jpy.array('java.lang.String[]', 1)) == ('ValueError', "Java class 'java.lang.String[]' not found")
assert raised(lambda: jpy.array(3, [1])) == ('ValueError', 'array: argument 1 (type) must be a type name or Java type object')
"#,
    );
}

/// `jpy.cast` of an array to its class, to a class it is no instance of, and of what is
/// no Java object; `jpy.convert` to an array class.
#[tokio::test]
async fn casts_and_conversions_of_arrays_answer_as_jpys() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven._java_time import NotPorted
from deephaven.time import to_j_instant, to_j_local_date
a = jpy.array('long', [1, 2])
s = jpy.array('java.lang.String', ['a'])
o = jpy.array('java.lang.Object', ['a'])
instant = to_j_instant('2024-01-01T00:00:00Z')
date = to_j_local_date('2024-01-01')
# jpy hands out a new object of the same array; here the array itself
assert jpy.cast(a, '[J') is a and jpy.cast(s, jpy.get_type('[Ljava.lang.String;')) is s
assert jpy.cast(instant, 'java.time.Instant') is instant and jpy.cast(date, 'java.time.LocalDate') is date
for value, names in ((a, ('[I', '[Ljava.lang.Object;', '[Ljava.lang.String;', 'java.lang.String', 'java.time.Instant', 'java.lang.Integer', 'java.lang.Long', 'java.time.LocalDate', 'org.jpy.PyObject')),
                     (s, ('[J', '[I', 'java.lang.String', 'java.time.Instant', 'java.lang.Integer', 'java.lang.Long', 'java.time.LocalDate', 'org.jpy.PyObject')),
                     (o, ('[J', '[I', '[Ljava.lang.String;', 'java.lang.String', 'java.time.Instant', 'java.lang.Integer', 'java.lang.Long', 'java.time.LocalDate', 'org.jpy.PyObject')),
                     (instant, ('[J', '[I', '[Ljava.lang.Object;', '[Ljava.lang.String;', 'java.lang.String', 'java.lang.Integer', 'java.lang.Long', 'java.time.LocalDate')),
                     (date, ('[J', '[I', '[Ljava.lang.Object;', '[Ljava.lang.String;', 'java.lang.String', 'java.time.Instant', 'java.lang.Integer', 'java.lang.Long'))):
    for name in names:
        assert jpy.cast(value, name) is None, (value, name)

# Java gives an object of the superclass, which has its methods only: refused here
for value, name in ((a, 'java.lang.Object'), (s, '[Ljava.lang.Object;'), (a, 'java.io.Serializable'),
                    (instant, 'java.lang.Comparable'), (date, 'java.lang.Object')):
    try:
        jpy.cast(value, name)
        raise AssertionError(name)
    except NotPorted as e:
        assert str(e).startswith(f'not ported: {name}: jpy.cast of a '), str(e)

not_java = ('ValueError', 'cast: argument 1 (obj) must be a Java object')
for value in ('x', 3, 1.5, True, [1], object()):
    assert raised(lambda: jpy.cast(value, '[J')) == not_java, value
assert raised(lambda: jpy.cast(a, 3)) == ('ValueError', 'cast: argument 2 (obj_type) must be a Java type name or Java type object')
assert jpy.cast(None, 3) is None and jpy.cast(None, 'nope.Nope') is None

# a conversion to an array class
made = jpy.convert([1, 2], '[J')
assert type(made).__name__ == '[J' and list(made) == [1, 2] and list(jpy.convert((1, 2), '[J')) == [1, 2]
assert type(jpy.convert([1, 2], '[I')).__name__ == '[I'
assert list(jpy.convert(['a'], '[Ljava.lang.String;')) == ['a']
assert jpy.convert(a, '[J') is a and jpy.convert(instant, 'java.time.Instant') is instant
assert raised(lambda: jpy.convert('a', 'java.time.Instant')) == ('ValueError', "cannot convert a Python 'str' to a Java 'java.time.Instant'")
"#,
    );
}

/// `deephaven.dtypes.array` makes the arrays `jpy.array` makes, and fails with the
/// package's text around jpy's.
#[tokio::test]
async fn dtypes_array_makes_java_arrays() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven import dtypes
def made(dtype, values):
    return type(dtypes.array(dtype, values)).__name__

assert made(dtypes.int32, [1, None]) == '[I' and list(dtypes.array(dtypes.int32, [1, None])) == [1, 0]
assert made(dtypes.bool_, [True, None]) == 'Boolean;' and list(dtypes.array(dtypes.bool_, [True, None])) == [True, None]
assert made(dtypes.string, ['a']) == 'String;' and made(dtypes.Instant, []) == 'Instant;'
assert [type(x).__name__ for x in dtypes.array(dtypes.Instant, ['2024-01-01T00:00:00Z', None])] == ['Instant', 'NoneType']
assert made(dtypes.long_array, [[1], None]) == '[[J'
assert [None if x is None else list(x) for x in dtypes.array(dtypes.long_array, [[1], None])] == [[1], None]
assert list(dtypes.array(dtypes.char, 'ab')) == [97, 98]
assert made(dtypes.PyObject, [[1]]) == 'PyObject;' and made(dtypes.JObject, [1]) == 'Object;'
assert made(dtypes.LocalDate, [None]) == 'LocalDate;' and made(dtypes.Duration, [None]) == 'Duration;'
assert made(dtypes.bool_array, [[True]]) == '[[Z'
assert list(dtypes.array(dtypes.int32, jpy.array('long', [5, 6]))) == [5, 6]
assert dtypes.array(dtypes.int32, None) is None
from deephaven.dherror import DHError
for dtype, values, text in ((dtypes.int32, ['x'], "failed to create a Java int array."),
                            (dtypes.string, [1], "failed to create a Java java.lang.String array."),
                            (dtypes.byte, [2**64], "failed to create a Java byte array.")):
    try:
        dtypes.array(dtype, values)
        raise AssertionError(values)
    except DHError as e:
        assert str(e).startswith(text), str(e)
# Java makes an array of the class it has; this package has no values of it
try:
    dtypes.array(dtypes.BigDecimal, [None])
    raise AssertionError('BigDecimal')
except DHError as e:
    assert 'not ported: java.math.BigDecimal[]: jpy.array' in str(e), str(e)
"#,
    );
}

/// A variable that holds a Java array is the array in every row of a formula, as in
/// Java: the array as a column, an element, its length, an argument of a function.
#[tokio::test]
async fn an_array_in_the_query_scope_is_a_value_of_formulas() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven import empty_table, dtypes
ja = jpy.array('long', [1, 2, 3])
js = jpy.array('java.lang.String', ['a', None, 'c'])
ji = jpy.array('int', [4, 5])
jd = jpy.array('double', [1.5])
jb = jpy.array('java.lang.Boolean', [True, None])
jc = jpy.array('char', [65])
je = jpy.array('long', 0)
dti = dtypes.array(dtypes.Instant, ['2024-01-01T00:00:00Z'])
t = empty_table(2).update(['A = ja', 'S = js', 'I = ji', 'D = jd', 'B = jb', 'C = jc', 'E = je'])
ti = empty_table(1).update(['T = dti', 'T0 = dti[0]'])
u = empty_table(2).update(['E1 = ja[1]', 'L = ja.length', 'S0 = js[0]', 'Sum = sum(ja)', 'N = isNull(ja)',
                           'R = ja[i]', 'Len = len(ja)', 'Cd = countDistinct(ja)', 'Same = (long[]) ja',
                           'SL = js.length', 'Plus = ii + ja[0]'])
v = empty_table(1).update_view(['c = ja', 's = js']).ungroup(['c', 's'])
"#,
    );
    assert_eq!(
        described(&python, "t"),
        [
            "A: Array(Long) = [1, 2, 3], [1, 2, 3]",
            "S: Array(String) = [a, , c], [a, , c]",
            "I: Array(Int) = [4, 5], [4, 5]",
            "D: Array(Double) = [1.5], [1.5]",
            "B: Array(Boolean) = [true, ], [true, ]",
            "C: Array(Char) = [65], [65]",
            "E: Array(Long) = [], []",
        ]
    );
    let instants = python.table("ti");
    let columns = instants.definition().columns();
    assert_eq!(
        columns[0].column_type,
        oxhaven_contracts::schema::ColumnType::array(
            oxhaven_contracts::schema::ColumnType::Instant
        )
    );
    assert_eq!(
        oxhaven_testkit::TableSnapshot::current(instants.as_ref())
            .unwrap()
            .columns[1]
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
            .value(0),
        1_704_067_200_000_000_000
    );
    assert_eq!(
        described(&python, "u"),
        [
            "E1: Long = 2, 2",
            "L: Int = 3, 3",
            "S0: String = a, a",
            "Sum: Long = 6, 6",
            "N: Boolean = false, false",
            "R: Long = 1, 2",
            "Len: Long = 3, 3",
            "Cd: Long = 3, 3",
            "Same: Array(Long) = [1, 2, 3], [1, 2, 3]",
            "SL: Int = 3, 3",
            "Plus: Long = 1, 2",
        ]
    );
    assert_eq!(
        described(&python, "v"),
        ["c: Long = 1, 2, 3", "s: String = a, , c"]
    );

    // Java reads past the end when a row is computed
    let text = error(&python, "empty_table(1).update('X = ja[5]')");
    assert!(
        text.contains("Index 5 out of bounds for length 3"),
        "{text}"
    );
    // Java makes these columns; the engine has no column type for them
    for (code, class) in [
        ("jpy.array('boolean', [True])", "boolean[]"),
        ("jpy.array('java.lang.Object', ['a'])", "java.lang.Object[]"),
    ] {
        let text = error(
            &python,
            &format!("arr = {code}\nempty_table(1).update('X = arr')"),
        );
        assert!(
            text.contains(&format!(
                "the query scope variable arr, a Java {class}: the engine has no column type for it"
            )),
            "{text}"
        );
    }

    // the array is read when the formula is compiled; Java's column holds the array and
    // shows a later change of it (docs/CONSOLE.md, "Java arrays")
    python.run("ja[0] = 99\nw = empty_table(1).update('X = ja')");
    assert_eq!(
        described(&python, "t")[0],
        "A: Array(Long) = [1, 2, 3], [1, 2, 3]"
    );
    assert_eq!(described(&python, "w"), ["X: Array(Long) = [99, 2, 3]"]);
}
