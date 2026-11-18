"""The `jpy` module of a server that has no Java VM.

The reference's Python reaches Java through jpy: `jpy.get_type` hands a script a Java class,
and the script calls its constructor, its static methods and its fields. This server has no
Java classes. The module is here so that a package which imports `jpy` and holds Java types
loads, as `deephaven.plot.express` does; every use of a type is refused by the name of the
Java class and, where the use names one, of its member. Decided by the owner on 2026-11-16
(`docs/CONSOLE.md`, "The jpy module"). The exceptions are the static methods in
`_BACKED`, which this server carries out (task 11.1), and the Java arrays of the classes
this package has values of, which `array`, `cast` and `convert` make and take (task 11.2).

The names are those of jpy 2.2.0, and what a call answers without Java was observed there,
inside the reference server and in a Python process without a Java VM.

Behavior: the `jpy` module (jpy 2.2.0), `org.jpy.PyLib`
"""

import array as _storage
import ctypes as _ctypes
import operator as _operator
import sys as _sys

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
        # the only Java objects of these classes are the arrays, whose classes extend
        # `java.lang.Object` as jpy's do
        return type.__instancecheck__(cls, instance)

    def __subclasscheck__(cls, other):
        if other is cls:
            return True
        if isinstance(other, _JavaClass) and issubclass(other, _JavaArray):
            # jpy gives an array class the one base `java.lang.Object`
            return type.__subclasscheck__(cls, other)
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
        bases = (JType,)
        component = _component(name)
        if component is not None and _supported(component):
            # an array of a class this package has values of: a class jpy's arrays are of
            storage = _PrimitiveArray if component in _PRIMITIVES else _ObjectArray
            bases = (get_type("java.lang.Object"), storage)
            attributes["__slots__"] = ()
        found = types.setdefault(name, _JavaClass(simple, bases, attributes))
    return found


# ------------------------------------------------------------------------------ arrays
#
# A Java array is an object of a class `get_type` hands out for the array's name (`[J`,
# `[Ljava.lang.String;`), as jpy's are. An array of a primitive type keeps its elements in
# an `array.array` of the same C type, so that it has the buffer of jpy's (`memoryview`,
# `numpy.frombuffer`); any other array keeps them in a list. What a script sees of either
# is what it sees of jpy's: a length, positions, Java's conversions of what is stored, and
# no more of Python's sequences. Every answer here was observed on jpy 2.2.0 in the
# reference server; `docs/CONSOLE.md`, "Java arrays", lists them.

# The primitive types: the letter of the JVM's name of an array of them, the C type of the
# storage, and for the integer types the bits and whether they have a sign.
_PRIMITIVES = {
    "boolean": ("Z", "B", None),
    "byte": ("B", "b", (8, True)),
    "short": ("S", "h", (16, True)),
    "char": ("C", "H", (16, False)),
    "int": ("I", "i", (32, True)),
    "long": ("J", "q", (64, True)),
    "float": ("F", "f", None),
    "double": ("D", "d", None),
}
_LETTERS = {letter: name for name, (letter, _, _) in _PRIMITIVES.items()}

# The boxed classes of the primitive types an element of their arrays is converted to:
# what Python value jpy takes, and the bits an integer keeps.
_BOXED_INTEGERS = {
    "java.lang.Byte": (8, True),
    "java.lang.Short": (16, True),
    "java.lang.Character": (16, False),
    "java.lang.Integer": (32, True),
    "java.lang.Long": (64, True),
}
_BOXED_REALS = {"java.lang.Float": True, "java.lang.Double": False}
_PLAIN_CLASSES = frozenset(
    {"java.lang.String", "java.lang.Boolean", "java.lang.Object", "org.jpy.PyObject"}
    | set(_BOXED_INTEGERS)
    | set(_BOXED_REALS)
)


_VALUE_CLASSES = None


def _value_classes() -> dict:
    """The classes of the package that stand for a Java class, by its name: the values of
    `java.time` and of the calendars, whose arrays can be made."""
    global _VALUE_CLASSES
    if _VALUE_CLASSES is not None:
        return _VALUE_CLASSES
    from deephaven import _calendar, _java_time

    classes = [
        _java_time.Instant,
        _java_time.LocalDate,
        _java_time.LocalTime,
        _java_time.ZonedDateTime,
        _java_time.Duration,
        _java_time.Period,
        _java_time.ZoneId,
        _java_time.DayOfWeek,
        _calendar.BusinessCalendar,
        _calendar.CalendarDay,
        _calendar.TimeRange,
    ]
    _VALUE_CLASSES = {value_class._java_name: value_class for value_class in classes}
    return _VALUE_CLASSES


def _component(name: str):
    """The name of the class of the elements of the array class `name` (`long`, `[J`,
    `java.lang.String`); None for a name that is no array's."""
    if not name.startswith("["):
        return None
    rest = name[1:]
    if rest in _LETTERS:
        return _LETTERS[rest]
    if rest.startswith("L") and rest.endswith(";") and len(rest) > 2:
        return rest[1:-1]
    if rest.startswith("[") and _component(rest) is not None:
        return rest
    return None


def _array_name(component: str) -> str:
    """The name of the class of an array of `component`, as the JVM names it."""
    if component in _PRIMITIVES:
        return "[" + _PRIMITIVES[component][0]
    if component.startswith("["):
        return "[" + component
    return f"[L{component};"


def _supported(component: str) -> bool:
    """Whether arrays of the class `component` are made here."""
    if component in _PRIMITIVES or component in _PLAIN_CLASSES:
        return True
    inner = _component(component)
    if inner is not None:
        return _supported(inner)
    return component in _value_classes()


def _is_java(value) -> bool:
    """Whether a value is one this package has for a Java object."""
    if isinstance(value, _JavaArray):
        return True
    java_name = getattr(type(value), "_java_name", None)
    return isinstance(java_name, str) and bool(java_name)


def _python_name(value) -> str:
    """The name jpy's texts give the type of a value: the Java class of a Java object, and
    the C name of a Python type, which names the module of a type not written in Python."""
    if isinstance(value, _JavaArray):
        return type(value).jclassname
    java_name = getattr(type(value), "_java_name", None)
    if isinstance(java_name, str) and java_name:
        return java_name
    python_type = type(value)
    module = python_type.__module__
    heap = python_type.__flags__ & (1 << 9)
    if heap or module == "builtins":
        return python_type.__name__
    return f"{module}.{python_type.__qualname__}"


def _refused(value, component: str) -> ValueError:
    return ValueError(f"cannot convert a Python '{_python_name(value)}' to a Java '{component}'")


def _is_sequence(value) -> bool:
    """Behavior: `PySequence_Check`, which jpy asks of an initializer: a type with items by
    position, a dict not"""
    return not isinstance(value, dict) and hasattr(type(value), "__getitem__")


def _items(init) -> list:
    """The items of a sequence by position, as jpy reads them."""
    return [init[index] for index in range(len(init))]


def _wrap(value: int, bits: int, signed: bool) -> int:
    value %= 2**bits
    return value - 2**bits if signed and value >= 2 ** (bits - 1) else value


def _primitive(component: str, value, failing):
    """A Python value as jpy stores it in an array of the primitive type `component`.

    `failing` is None where jpy makes an array (a value it cannot convert fails) and the
    value jpy stores instead where it sets an element: it sets -1, and no error, for a
    value an integer or real type cannot take.

    Behavior: jpy's conversions `PyLong_AsLongLong` and `PyFloat_AsDouble`, then a C cast
    """
    _, _, integer = _PRIMITIVES[component]
    if value is None:
        return False if component == "boolean" else 0
    if component == "boolean":
        return bool(value)
    if integer is not None:
        try:
            number = _operator.index(value)
        except TypeError:
            if failing is not None:
                return failing
            raise
        if not -(2**63) <= number < 2**63:
            if failing is not None:
                return failing
            raise OverflowError(
                "int too big to convert" if component == "long" else "Python int too large to convert to C long"
            )
        return _wrap(number, *integer)
    try:
        if hasattr(type(value), "__float__"):
            return float(value)
        if hasattr(type(value), "__index__"):
            return float(_operator.index(value))
        raise TypeError(f"must be real number, not {type(value).__name__}")
    except (TypeError, OverflowError):
        if failing is not None:
            return failing
        raise


def _element(component: str, value, overflowed):
    """A Python value as jpy stores it in an array of the class `component`, or the
    `ValueError` of jpy for one it cannot convert. `overflowed` is the `SystemError` jpy
    leaves for an `int` beyond 64 bits where it boxes one.

    Behavior: jpy's conversion of a Python value to a Java object of a class
    """
    if value is None:
        return None
    if component == "org.jpy.PyObject":
        return value
    if component == "java.lang.String":
        if isinstance(value, str):
            return str(value)
        raise _refused(value, component)
    if component == "java.lang.Boolean":
        return bool(value)
    if component in _BOXED_INTEGERS:
        if not isinstance(value, int):
            raise _refused(value, component)
        if not -(2**63) <= value < 2**63:
            raise overflowed()
        return _wrap(int(value), *_BOXED_INTEGERS[component])
    if component in _BOXED_REALS:
        if not isinstance(value, (int, float)):
            raise _refused(value, component)
        real = float(value)
        return _ctypes.c_float(real).value if _BOXED_REALS[component] else real
    if component == "java.lang.Object":
        if _is_java(value):
            return value
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, int):
            if not -(2**63) <= value < 2**63:
                raise overflowed()
            return int(value)
        if isinstance(value, float):
            return float(value)
        if isinstance(value, str):
            return str(value)
        raise _refused(value, component)
    if component.startswith("["):
        # an array of the element class is the element itself; a sequence becomes one
        if type(value) is get_type(component):
            return value
        if _is_sequence(value):
            return _make(component, value)
        raise ValueError(
            f"cannot convert a Python '{_python_name(value)}' to a Java array of type '{_component(component)}'"
        )
    if isinstance(value, _value_classes()[component]):
        return value
    raise _refused(value, component)


def _overflowed(where: str):
    return lambda: SystemError(f"{where} returned a result with an exception set")


def _make(name: str, init):
    """A new array of the class `name` (`[J`): of `init` elements of the default value
    if it is an `int`, else of the items of the sequence `init`.

    Behavior: `jpy.array` (jpy 2.2.0)
    """
    component = _component(name)
    array_class = get_type(name)
    if isinstance(init, int):
        if not 0 <= init < 2**31:
            raise _bad_init()
        if component in _PRIMITIVES:
            values = [0] * init
        else:
            values = [None] * init
    elif _is_sequence(init):
        items = _items(init)
        if component in _PRIMITIVES:
            values = [_primitive(component, item, None) for item in items]
        else:
            overflowed = _overflowed("<built-in function array>")
            values = [_element(component, item, overflowed) for item in items]
    else:
        raise _bad_init()
    if component in _PRIMITIVES:
        return _storage.array.__new__(array_class, _PRIMITIVES[component][1], values)
    made = object.__new__(array_class)
    object.__setattr__(made, "_items", values)
    return made


def _bad_init() -> ValueError:
    return ValueError("array: argument 2 (init) must be either an integer array length or any sequence")


_JAVA_OBJECT_METHODS = ("equals", "getClass", "hashCode", "jclass", "jclassname", "notify", "notifyAll", "toString", "wait")


class _JavaArray:
    """What every Java array has, whatever keeps its elements.

    Behavior: jpy's Java objects (`JObj`) and its sequence methods for arrays
    """

    __slots__ = ()

    # -- positions

    def _position(self, index) -> int:
        """The position `index` names, from the end if negative, as Python's sequences
        hand it to jpy; out of the array where it is out of it."""
        if not hasattr(type(index), "__index__"):
            raise TypeError(f"sequence index must be integer, not '{type(index).__name__}'")
        index = _operator.index(index)
        if not -_sys.maxsize - 1 <= index <= _sys.maxsize:
            raise IndexError(f"cannot fit '{type(index).__name__}' into an index-sized integer")
        return index + len(self) if index < 0 else index

    def __getitem__(self, index):
        position = self._position(index)
        if not 0 <= position < len(self):
            raise IndexError("Java array index out of bounds")
        return self._get(position)

    def __setitem__(self, index, value):
        self._set(self._position(index), value)

    def __delitem__(self, index):
        _operator.index(index)
        raise RuntimeError("cannot delete items of Java arrays")

    def __setattr__(self, name, value):
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __iter__(self):
        for position in range(len(self)):
            yield self._get(position)

    def __contains__(self, value):
        return any(item is value or item == value for item in self)

    # -- what a Java object has

    def __eq__(self, other):
        if not _is_java(other):
            return False
        return self is other

    def __ne__(self, other):
        if not _is_java(other):
            return False
        return self is not other

    # jpy orders two Java objects that are not `Comparable` by their references, and
    # answers False for anything else; here two arrays are ordered by their identities
    def _order(self, other, outcome):
        if not _is_java(other):
            return False
        return outcome(id(self), id(other))

    def __lt__(self, other):
        return self._order(other, _operator.lt)

    def __le__(self, other):
        return self._order(other, _operator.le)

    def __gt__(self, other):
        return self._order(other, _operator.gt)

    def __ge__(self, other):
        return self._order(other, _operator.ge)

    def _identity(self) -> int:
        """The identity hash code Java gives the array: 31 bits."""
        return (id(self) >> 4) & 0x7FFFFFFF

    def __hash__(self):
        return self._identity()

    def __str__(self):
        return f"{type(self).jclassname}@{self._identity():x}"

    def __repr__(self):
        return f"{type(self).jclassname}(objectRef=0x{id(self) & 0xFFFFFFFF:x})"

    def __reduce_ex__(self, protocol):
        raise TypeError(f"cannot pickle '{type(self).__name__}' object")

    def __copy__(self):
        return self.__reduce_ex__(2)

    def __deepcopy__(self, memo):
        return self.__reduce_ex__(2)

    def __add__(self, other):
        return NotImplemented

    __radd__ = __iadd__ = __mul__ = __rmul__ = __imul__ = __add__

    def __dir__(self):
        names = {name for name in object.__dir__(self) if name.startswith("__")}
        return sorted(names - _STORAGE_NAMES | set(_JAVA_OBJECT_METHODS))

    def equals(self, other):
        return self is other

    def hashCode(self):
        return self._identity()

    def toString(self):
        return str(self)

    def __getattr__(self, name):
        if name in _JAVA_OBJECT_METHODS:
            raise _not_ported(f"{type(self).jclassname}#{name}")
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


# what `array.array` has and a Java array has not
_STORAGE_NAMES = frozenset(name for name in dir(_storage.array) if not name.startswith("__")) | {
    "__class_getitem__",
}


class _PrimitiveArray(_JavaArray, _storage.array):
    """An array of a primitive type, whose elements are kept in the `array.array` it is."""

    __slots__ = ()

    def __getattribute__(self, name):
        if name in _STORAGE_NAMES:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return object.__getattribute__(self, name)

    def _component(self) -> str:
        return _component(type(self).jclassname)

    def _get(self, position):
        value = _storage.array.__getitem__(self, position)
        return bool(value) if self._component() == "boolean" else value

    def _set(self, position, value):
        component = self._component()
        failing = -1.0 if component in ("float", "double") else -1
        stored = _primitive(component, value, failing)
        if component == "char" and stored == -1:
            stored = 0xFFFF
        if not 0 <= position < len(self):
            raise RuntimeError(
                f"java.lang.ArrayIndexOutOfBoundsException: Array region {position}..{position + 1} "
                f"out of bounds for length {len(self)}"
            )
        _storage.array.__setitem__(self, position, stored)


class _ObjectArray(_JavaArray):
    """An array of a class, whose elements are kept in a list."""

    __slots__ = ("_items",)

    def __len__(self):
        return len(self._items)

    def _get(self, position):
        return self._items[position]

    def _set(self, position, value):
        where = f"<method-wrapper '__setitem__' of {type(self).jclassname} object at 0x{id(self):x}>"
        stored = _element(_component(type(self).jclassname), value, _overflowed(where))
        if not 0 <= position < len(self):
            raise RuntimeError(
                f"java.lang.ArrayIndexOutOfBoundsException: Index {position} out of bounds for length {len(self)}"
            )
        self._items[position] = stored


def _element_name(java_type, call: str, argument: str) -> str:
    """The name of the Java class an argument of `call` names, a type of `get_type` or a
    name; jpy's texts for anything else and for a name no class has."""
    if isinstance(java_type, _JavaClass):
        name = java_type.jclassname
    elif isinstance(java_type, str):
        name = java_type.replace("/", ".")
    else:
        raise ValueError(f"{call}: argument {argument}")
    if not name or "[]" in name or (name.startswith("[") and _component(name) is None):
        # no class has such a name
        raise ValueError(f"Java class '{name}' not found")
    return name


def array(name, init, /):
    """array(name, init) - Return a new Java array of given Java type (type name or type
    object) and initializer (array length or sequence).

    Here: an array of a primitive type, `String`, `Boolean`, the boxed numbers, `Object`,
    `org.jpy.PyObject`, a class of `java.time` or a calendar this package has, or of arrays
    of these. An array of another class is refused, by the name of the array's class.
    """
    component = _element_name(name, "array", "1 (type) must be a type name or Java type object")
    if not _supported(component):
        raise _not_ported(f"{component}[]", "jpy.array")
    return _make(_array_name(component), init)


# The superclasses and interfaces of every array class; any other class an array is not
# an instance of.
_ARRAY_SUPERTYPES = ("java.lang.Object", "java.lang.Cloneable", "java.io.Serializable")


def _final(name: str) -> bool:
    """Whether `name` is a final class this package knows, which has no subclass: the
    boxed numbers, `String`, `Boolean` and the classes of the package's values but
    `ZoneId`, which Java's zones extend."""
    plain = name in _PLAIN_CLASSES and name not in ("java.lang.Object", "org.jpy.PyObject")
    return plain or (name in _value_classes() and name != "java.time.ZoneId")


def _known(name: str) -> bool:
    """Whether `name` is a class whose relation to the package's Java objects is known."""
    if name in _PLAIN_CLASSES or name in _ARRAY_SUPERTYPES or name in _value_classes():
        return True
    component = _component(name)
    return component is not None and _supported(component)


def _instance_of(value, name: str):
    """Whether the Java object `value` is an instance of the class `name`: True, False, or
    None where that needs Java's classes."""
    if isinstance(value, _JavaArray):
        return _array_class_extends(type(value).jclassname, name)
    own = type(value)._java_name
    if name == own or name == "java.lang.Object":
        return True
    if _component(name) is not None:
        # an object that is no array is no array
        return False
    if _final(own) and _final(name):
        return False
    return None


def _array_class_extends(own: str, name: str):
    """Whether an array of the class `own` is an instance of the class `name`.

    Behavior: the Java language's rules for arrays: an array is an `Object`, `Cloneable`
    and `Serializable` and nothing else, and an array of a class is an array of each of
    the class's supertypes
    """
    if own == name or name in _ARRAY_SUPERTYPES:
        return True
    target = _component(name)
    if target is None:
        return False
    mine = _component(own)
    if mine in _PRIMITIVES or target in _PRIMITIVES:
        return False
    if target == "java.lang.Object":
        return True
    if mine == "java.lang.Object":
        # `Object` has no superclass
        return False
    if mine.startswith("["):
        return _array_class_extends(mine, target)
    if _final(mine) and (_final(target) or target.startswith("[")):
        return False
    return None


def _native(value) -> bool:
    """Whether a value is a handle of the server, which stands for a Java object in the
    reference (a table, a figure)."""
    return type(value).__module__ == "_oxhaven"


def cast(obj, type, /):
    """cast(obj, type) - Cast the given Java object to the given Java type (type name or
    type object). Returns None if the cast is not possible.

    Here: None for None, as in jpy, which does not look at the type then. A Java array or
    another of the package's Java values cast to its own class is itself, and to a class
    it is no instance of None. A cast to a superclass, which gives an object that has
    only the superclass's methods, and a cast that needs Java's classes are refused.
    """
    if obj is None:
        return None
    name = _element_name(type, "cast", "2 (obj_type) must be a Java type name or Java type object")
    if _native(obj):
        raise _not_ported(name, f"jpy.cast of a {_python_name(obj)}")
    if not _is_java(obj):
        raise ValueError("cast: argument 1 (obj) must be a Java object")
    own = _python_name(obj)
    extends = _instance_of(obj, name) if _known(name) else None
    if extends is False:
        return None
    if extends is True and name == own:
        return obj
    raise _not_ported(name, f"jpy.cast of a {own}")


def convert(obj, type, /):
    """convert(obj, type) - Convert the given Python object to the given Java type (type
    name or type object).

    Here: to an array class, as an element of an array of arrays is converted: `None` is
    None, an array of the class is itself, and a sequence is a new array of its items. To
    a class of the package's Java values: `None` is None, a value of the class is itself.
    Every other conversion, which gives a Java object this package has no value for, is
    refused by the name of the class.
    """
    # jpy's text names `cast` here too
    name = _element_name(type, "cast", "2 (obj_type) must be a Java type name or Java type object")
    component = _component(name)
    if component is not None and _supported(component):
        return _element(name, obj, _overflowed("<built-in function convert>"))
    if name in _value_classes():
        return _element(name, obj, None)
    raise _not_ported(name, "jpy.convert")


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
