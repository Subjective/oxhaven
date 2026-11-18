//! Tests of the `jpy` module the binary holds (`python/jpy.py`): it imports, a type is held,
//! and every use of a type is refused by the name of its Java class.
//!
//! What jpy 2.2.0 answers without reaching a Java class (the names of the module, the name
//! and text of a type, `cast` of None, `has_jvm` and `destroy_jvm` without a Java VM, the
//! texts of its argument checks) was observed in the reference's Python and is expected
//! here as observed.

use super::tests::Python;

const SETUP: &str = r#"
import jpy

def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e)
    return None
"#;

/// A package that imports `jpy` and holds types at import loads: nothing of it is a use.
#[tokio::test]
async fn jpy_imports_and_holds_a_type() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
names = sorted(name for name in dir(jpy) if not name.startswith('_'))
assert names == ['JException', 'JField', 'JMethod', 'JOverloadedMethod', 'JType',
                 'VerboseExceptions', 'array', 'byte_buffer', 'cast', 'convert', 'create_jvm',
                 'destroy_jvm', 'diag', 'get_type', 'has_jvm', 'type_callbacks',
                 'type_translations', 'types'], names
assert jpy.has_jvm() is False and jpy.destroy_jvm() is None

held = jpy.get_type('io.deephaven.time.DateTimeUtils')
assert repr(held) == str(held) == "<class 'io.deephaven.time.DateTimeUtils'>"
assert (held.__name__, held.__qualname__, held.__module__) == ('DateTimeUtils', 'DateTimeUtils', 'io.deephaven.time')
assert held.jclassname == 'io.deephaven.time.DateTimeUtils'
assert jpy.get_type('io.deephaven.time.DateTimeUtils') is held
assert jpy.get_type('io/deephaven/time/DateTimeUtils') is held
assert jpy.get_type('io.deephaven.time.DateTimeUtils', False) is held
assert jpy.types['io.deephaven.time.DateTimeUtils'] is held
assert isinstance(held, type) and issubclass(held, jpy.JType) and issubclass(held, held)
assert not isinstance(3, held) and not isinstance(held, jpy.JType) and not issubclass(int, held)
assert held == held and held != jpy.get_type('java.lang.Object') and {held: 1}[held] == 1
assert repr(jpy.get_type('int')) == "<class 'int'>" and jpy.get_type('int') is not int
assert jpy.get_type('java.util.Map$Entry').__name__ == 'Map$Entry'

# as a type hint, a default, a member of a collection: no use
def takes(value: held = None, *more: held) -> held:
    return value
assert takes() is None and [held][0] is held

# what Python asks of any class is answered as by a class
assert not hasattr(held, '__wrapped__') and getattr(held, '__iter__', 5) == 5
assert raised(lambda: held.__nope__) == ('AttributeError', "type object 'io.deephaven.time.DateTimeUtils' has no attribute '__nope__'")
import copy, inspect
assert copy.deepcopy(held) is held and inspect.isclass(held)

# the checks jpy makes of the arguments of get_type
assert raised(lambda: jpy.get_type(3)) == ('TypeError', 'get_type() argument 1 must be str, not int')
assert raised(lambda: jpy.get_type('')) == ('ValueError', "Java class '' not found")
assert raised(lambda: jpy.get_type('a.B', 'x')) == ('TypeError', "'str' object cannot be interpreted as an integer")

assert issubclass(jpy.JException, Exception)
assert (jpy.diag.flags, jpy.diag.F_ALL, jpy.diag.F_ERR, jpy.diag.F_OFF) == (0, 255, 32, 0)
assert jpy.VerboseExceptions.enabled is False
jpy.VerboseExceptions.enabled = True
assert jpy.VerboseExceptions.enabled is True
jpy.VerboseExceptions.enabled = False
assert raised(lambda: setattr(jpy.VerboseExceptions, 'enabled', 'x')) == ('ValueError', "value for 'flags' must be a boolean")
assert jpy.type_callbacks == {} and jpy.type_translations == {}
"#,
    );
}

/// Each kind of use fails with the refusal of this package, which names the Java class
/// and the member the use names.
#[tokio::test]
async fn every_use_of_a_jpy_type_is_refused_by_the_name_of_its_class() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven._java_time import NotPorted
held = jpy.get_type('io.deephaven.time.DateTimeUtils')
for call, text in (
    # a constructor
    (lambda: held(), 'io.deephaven.time.DateTimeUtils#<init>'),
    (lambda: held(1, zone='UTC'), 'io.deephaven.time.DateTimeUtils#<init>'),
    # a static method, called or only taken; a static field (timeZone and toLocalTime are
    # carried out, `calendar_tests.rs`)
    (lambda: held.parseInstant('2024-07-03T13:30:00Z'), 'io.deephaven.time.DateTimeUtils#parseInstant'),
    (lambda: held.toLocalDate, 'io.deephaven.time.DateTimeUtils#toLocalDate'),
    (lambda: held.MILLI, 'io.deephaven.time.DateTimeUtils#MILLI'),
    (lambda: getattr(held, 'SECOND', 1), 'io.deephaven.time.DateTimeUtils#SECOND'),
    (lambda: hasattr(held, 'now'), 'io.deephaven.time.DateTimeUtils#now'),
    (lambda: held._private, 'io.deephaven.time.DateTimeUtils#_private'),
    # the Java class itself
    (lambda: held.jclass, 'io.deephaven.time.DateTimeUtils#jclass'),
    # whether one class extends another
    (lambda: issubclass(held, jpy.get_type('java.lang.Object')),
     'io.deephaven.time.DateTimeUtils#getSuperclass: whether it extends java.lang.Object'),
    # arrays, casts, conversions, by a name and by a type
    (lambda: jpy.array('java.lang.String', ['a']), 'java.lang.String[]: jpy.array'),
    (lambda: jpy.array('long', 3), 'long[]: jpy.array'),
    (lambda: jpy.array(held, 0), 'io.deephaven.time.DateTimeUtils[]: jpy.array'),
    (lambda: jpy.cast(object(), held), 'io.deephaven.time.DateTimeUtils: jpy.cast'),
    (lambda: jpy.cast(3, 'java.util.List'), 'java.util.List: jpy.cast'),
    (lambda: jpy.convert('a', 'java.lang.String'), 'java.lang.String: jpy.convert'),
    (lambda: jpy.convert(None, held), 'io.deephaven.time.DateTimeUtils: jpy.convert'),
    (lambda: jpy.array(3, 3), 'jpy.array: a Java type given as int'),
    # no class to name
    (lambda: jpy.byte_buffer(b'ab'), 'java.nio.ByteBuffer: jpy.byte_buffer'),
    (lambda: jpy.create_jvm([]), 'jpy.create_jvm: this server has no Java VM'),
):
    try:
        call()
    except NotPorted as e:
        # the refusal is the `RuntimeError` that code written for jpy catches
        assert isinstance(e, (RuntimeError, NotImplementedError)) and str(e) == 'not ported: ' + text, str(e)
    else:
        raise AssertionError('no refusal: ' + text)

# jpy does not look at the type when there is nothing to cast
assert jpy.cast(None, held) is None and jpy.cast(None, 'no.such.Class') is None
# a type cannot be changed, as jpy's cannot
immutable = "cannot set 'timeZone' attribute of immutable type 'io.deephaven.time.DateTimeUtils'"
assert raised(lambda: setattr(held, 'timeZone', 1)) == ('TypeError', immutable)
assert raised(lambda: delattr(held, 'timeZone')) == ('TypeError', immutable)
assert raised(lambda: jpy.array('int')) is not None and raised(lambda: jpy.cast()) is not None

# a module that uses a type in its body fails where it is used, and by the name
import sys, types
module = types.ModuleType('uses_java_at_import')
source = "import jpy\n_JDouble = jpy.get_type('java.lang.Double')\nLIMIT = _JDouble.MAX_VALUE\n"
assert raised(lambda: exec(source, module.__dict__)) == ('NotPorted', 'not ported: java.lang.Double#MAX_VALUE')
assert '_JDouble' in module.__dict__ and 'LIMIT' not in module.__dict__
"#,
    );
}
