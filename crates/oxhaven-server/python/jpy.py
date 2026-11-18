"""The `jpy` module of a server that has no Java VM.

The reference's Python reaches Java through jpy: `jpy.get_type` hands a script a Java class,
and the script calls its constructor, its static methods and its fields. This server has no
Java classes. The module is here so that a package which imports `jpy` and holds Java types
loads, as `deephaven.plot.express` does; every use of a type is refused by the name of the
Java class and, where the use names one, of its member. Decided by the owner on 2026-11-16
(`docs/CONSOLE.md`, "The jpy module"). The exceptions are the static methods in
`_BACKED`, which this server carries out (task 11.1).

The names are those of jpy 2.2.0, and what a call answers without Java was observed there,
inside the reference server and in a Python process without a Java VM.

Behavior: the `jpy` module (jpy 2.2.0), `org.jpy.PyLib`
"""

import operator as _operator

__all__ = [
    "JException",
    "JField",
    "JMethod",
    "JOverloadedMethod",
    "JType",
    "VerboseExceptions",
    "array",
    "byte_buffer",
    "cast",
    "convert",
    "create_jvm",
    "destroy_jvm",
    "diag",
    "get_type",
    "has_jvm",
    "type_callbacks",
    "type_translations",
    "types",
]


def _not_ported(java_name: str, what: str = "") -> Exception:
    """The refusal of this package for what Java does and this server does not: a
    `NotImplementedError`, and so a `RuntimeError`, which is what jpy raises for a Java
    exception."""
    from deephaven._java_time import NotPorted

    return NotPorted(java_name, what)


class JType:
    """Java Meta Type"""


class JField:
    """Java Field Wrapper"""


class JMethod:
    """Java Method Wrapper"""


class JOverloadedMethod:
    """Java Overloaded Method"""


class JException(Exception):
    """What jpy raises for a Java exception when it is told to keep the Java object. No
    Java exception is raised here, so nothing raises it; code that catches it runs."""


def _backed(java_class: str, name: str):
    """The function that carries out a static method of a Java class, or None.

    Two methods of `io.deephaven.time.DateTimeUtils`, the ones `deephaven.plot.express`
    calls for a figure with a business calendar (`deephaven._calendar.date_time_utils`).
    """
    if java_class == "io.deephaven.time.DateTimeUtils":
        from deephaven import _calendar

        return _calendar.date_time_utils.get(name)
    if java_class == "io.deephaven.engine.table.impl.util.ColumnsToRowsTransform" and name == "columnsToRows":
        # what `dx.data.marketing` of `deephaven.plot.express` calls (task 11.3)
        from deephaven import _columns_to_rows

        return _columns_to_rows.columnsToRows
    return None


class _JavaClass(type):
    """The class of the types `get_type` hands out. A type has a name and nothing else:
    whatever would reach the Java class is refused.

    jpy's types are instances of `type` itself, with the members of the Java class as
    attributes.
    """

    def __getattr__(cls, name):
        # what Python itself asks of a class (`__wrapped__`, `__iter__`, `__set_name__`) is
        # no Java member, and the code that asks expects an AttributeError
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(f"type object '{cls.jclassname}' has no attribute '{name}'")
        backed = _backed(cls.jclassname, name)
        if backed is not None:
            return backed
        raise _not_ported(f"{cls.jclassname}#{name}")

    def __call__(cls, *args, **kwargs):
        raise _not_ported(f"{cls.jclassname}#<init>")

    def __setattr__(cls, name, value):
        raise TypeError(f"cannot set '{name}' attribute of immutable type '{cls.jclassname}'")

    def __delattr__(cls, name):
        raise TypeError(f"cannot set '{name}' attribute of immutable type '{cls.jclassname}'")

    def __instancecheck__(cls, instance):
        # there is no Java object in this process
        return False

    def __subclasscheck__(cls, other):
        if other is cls:
            return True
        if isinstance(other, _JavaClass):
            # jpy answers by the superclasses of the Java class
            raise _not_ported(f"{other.jclassname}#getSuperclass", f"whether it extends {cls.jclassname}")
        return False


types = {}
"""The types handed out so far, by the name of their Java class."""

type_callbacks = {}
type_translations = {}


def get_type(name, resolve=True):
    """get_type(name, resolve=True) - Return the Java class with the given name, e.g.
    'java.io.File'.

    Here: a type that stands for the class of that name, the same one for the same name.
    No class is looked up, so a name that Java has no class for is given a type too, where
    jpy raises `ValueError: Java class '<name>' not found`; the name is in the refusal of
    every use.
    """
    if not isinstance(name, str):
        raise TypeError(f"get_type() argument 1 must be str, not {type(name).__name__}")
    _operator.index(resolve)
    if not name:
        raise ValueError("Java class '' not found")
    name = name.replace("/", ".")
    found = types.get(name)
    if found is None:
        package, _, simple = name.rpartition(".")
        attributes = {"__module__": package or "builtins", "__qualname__": simple, "jclassname": name}
        found = types.setdefault(name, _JavaClass(simple, (JType,), attributes))
    return found


def _class_name(java_type, call: str) -> str:
    """The name of the Java class an argument names: a type of `get_type` or a name."""
    if isinstance(java_type, _JavaClass):
        return java_type.jclassname
    if isinstance(java_type, str):
        return java_type.replace("/", ".")
    raise _not_ported(f"jpy.{call}", f"a Java type given as {type(java_type).__name__}")


def array(name, init, /):
    """array(name, init) - Return a new Java array of given Java type (type name or type
    object) and initializer (array length or sequence).

    Here: refused, by the name of the array's class.
    """
    raise _not_ported(f"{_class_name(name, 'array')}[]", "jpy.array")


def cast(obj, type, /):
    """cast(obj, type) - Cast the given Java object to the given Java type (type name or
    type object). Returns None if the cast is not possible.

    Here: None for None, as in jpy, which does not look at the type then. Anything else is
    refused, by the name of the class.
    """
    if obj is None:
        return None
    raise _not_ported(_class_name(type, "cast"), "jpy.cast")


def convert(obj, type, /):
    """convert(obj, type) - Convert the given Python object to the given Java type (type
    name or type object).

    Here: refused, by the name of the class.
    """
    raise _not_ported(_class_name(type, "convert"), "jpy.convert")


def byte_buffer(obj, /):
    """byte_buffer(obj) - Return a new Java direct ByteBuffer sharing the same underlying,
    contiguous buffer of obj via its implemented Buffer Protocol.

    Here: refused.
    """
    raise _not_ported("java.nio.ByteBuffer", "jpy.byte_buffer")


def has_jvm():
    """has_jvm() - Check if the JVM is available. Here: it is not."""
    return False


def create_jvm(options):
    """create_jvm(options) - Create the Java VM from the given list of options.

    Here: refused.
    """
    raise _not_ported("jpy.create_jvm", "this server has no Java VM")


def destroy_jvm(*args):
    """destroy_jvm() - Destroy the current Java VM. Nothing to do without one, as in jpy."""
    return None


class _Diag:
    """Controls output of diagnostic information for debugging"""

    __slots__ = ("flags",)
    F_OFF = 0x00
    F_TYPE = 0x01
    F_METH = 0x02
    F_EXEC = 0x04
    F_MEM = 0x08
    F_JVM = 0x10
    F_ERR = 0x20
    F_ALL = 0xFF

    def __init__(self):
        self.flags = 0


class _VerboseExceptions:
    """Controls python exception verbosity"""

    __slots__ = ("_enabled",)

    def __init__(self):
        self._enabled = False

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        if not isinstance(value, bool):
            raise ValueError("value for 'flags' must be a boolean")
        self._enabled = value


_Diag.__name__ = _Diag.__qualname__ = "Diag"
_VerboseExceptions.__name__ = _VerboseExceptions.__qualname__ = "VerboseExceptions"
diag = _Diag()
VerboseExceptions = _VerboseExceptions()
