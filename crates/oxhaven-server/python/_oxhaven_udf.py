"""Python functions called from query strings: what a function's signature says about a call,
and the function that runs it for a chunk of rows.

The server calls `prepare` once for each call in a formula, when the formula is compiled
(`src/console/python/udf.rs`). `docs/PYTHON_UDF.md` has the design, what Java does, and why
each refusal here is one. Types are named as Java names them (`long`, `java.lang.String`),
because that is how the engine and the messages name them.
"""

import collections.abc
import datetime
import inspect
import sys
import types
import typing

BOOLEAN = "java.lang.Boolean"
STRING = "java.lang.String"
INSTANT = "java.time.Instant"
OBJECT = "java.lang.Object"
NULL = "null"
ARRAY = "an array"

# The integer types, with the value Java reserves for null in each.
_INTEGER_NULLS = {"byte": -(2**7), "short": -(2**15), "char": 2**16 - 1, "int": -(2**31), "long": -(2**63)}
# The null values of the floating point types, as the Python floats a function is given.
_REAL_NULLS = {"float": -3.4028234663852886e38, "double": -sys.float_info.max}
_NULLS = {**_INTEGER_NULLS, **_REAL_NULLS}

# Behavior: `PyCallableWrapperJpyImpl#isLosslessWideningPrimitiveConversion`
_JAVA_WIDENS = {
    "byte": ("short", "int", "long"),
    "short": ("int", "long"),
    "char": ("int", "long"),
    "int": ("long",),
    "float": ("double",),
}
# Behavior: `deephaven._udf#_is_lossless_convertible`, which also takes a `byte` for a `char`
_CONVERTER_WIDENS = {**_JAVA_WIDENS, "byte": ("short", "char", "int", "long")}

# The numpy type characters that name a column type.
_NUMPY_CHARS = {
    "b": "byte", "h": "short", "H": "char", "i": "int", "l": "long", "f": "float", "d": "double",
    "?": BOOLEAN, "U": STRING, "M": INSTANT,
}
_BUILTINS = ((int, "long"), (float, "double"), (bool, BOOLEAN), (str, STRING))
_DTYPE_BUILTINS = {
    "byte": int, "short": int, "char": int, "int": int, "long": int, "float": float, "double": float,
    BOOLEAN: bool, STRING: str,
}


class _Refused(Exception):
    """A call Java makes and this server does not: `java` is the method that does the work."""

    def __init__(self, what, java):
        super().__init__(f"{what}: not ported: {java}")


class _Mismatch(Exception):
    """A call Java refuses too."""


def _unalias(annotation):
    """The type a `type X = ...` statement named."""
    alias = getattr(typing, "TypeAliasType", None)
    seen = set()
    while alias is not None and isinstance(annotation, alias) and id(annotation) not in seen:
        seen.add(id(annotation))
        annotation = annotation.__value__
    return annotation


def _members(annotation):
    """The types of a union, or the annotation alone."""
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is getattr(types, "UnionType", None):
        return [_unalias(member) for member in typing.get_args(annotation)]
    return [annotation]


def _builtin(annotation):
    """The Java type of `int`, `float`, `bool` or `str`; `None` for anything else. By identity:
    an annotation can be any object, and some compare equal to a type they are not."""
    for builtin, java in _BUILTINS:
        if annotation is builtin:
            return java
    return None


def _numpy():
    """numpy, if the session has imported it: no annotation names a numpy type otherwise."""
    return sys.modules.get("numpy")


def _is_dtype(annotation) -> bool:
    dtypes = sys.modules.get("deephaven.dtypes")
    return dtypes is not None and isinstance(annotation, dtypes.DType)


def _is_array(annotation) -> bool:
    """Whether the annotation names a sequence of elements or a numpy array."""
    if annotation is bytes or annotation is bytearray:
        return True
    origin = _unalias(typing.get_origin(annotation))
    origin = typing.get_origin(origin) or origin
    numpy = _numpy()
    if numpy is not None and (origin is numpy.ndarray or annotation is numpy.ndarray):
        return True
    if isinstance(origin, type) and issubclass(origin, collections.abc.Sequence) and typing.get_args(annotation):
        return True
    return _is_dtype(annotation) and annotation.j_name.startswith("[")


def _java_type(annotation):
    """The Java type an annotation names, with the Python type that converts a value to what
    the annotation asks for; `(None, None)` for an annotation that names no type Java knows.

    Behavior: `deephaven._udf#_np_dtype_char` and
    `PyCallableWrapperJpyImpl#numpyType2JavaClass`
    """
    if annotation is None or annotation is type(None):
        return NULL, None
    if annotation is typing.Any or annotation is object:
        return OBJECT, object
    if _is_array(annotation):
        return ARRAY, None
    numpy = _numpy()
    if _is_dtype(annotation):
        name = annotation.j_name
        if name == INSTANT:
            return INSTANT, None
        if name not in _DTYPE_BUILTINS:
            return None, None
        # the numpy type of the DType, as the reference converts with it
        effective = annotation.np_type if numpy is not None else _DTYPE_BUILTINS[name]
        return name, effective
    if _builtin(annotation) is not None:
        return _builtin(annotation), annotation
    if annotation is datetime.datetime:
        return INSTANT, None
    pandas = sys.modules.get("pandas")
    if pandas is not None and annotation is pandas.Timestamp:
        return INSTANT, None
    if numpy is not None and isinstance(annotation, type) and issubclass(annotation, numpy.generic):
        try:
            name = _NUMPY_CHARS.get(numpy.dtype(annotation).char)
        except Exception:
            name = None
        return name, (annotation if name is not None else None)
    return None, None


class _Parameter:
    """One parameter of a function: the types its annotation names.

    Behavior: `deephaven._udf#_parse_param`
    """

    def __init__(self, name, annotation):
        self.name = name
        if annotation is inspect.Parameter.empty:
            self.members = [(OBJECT, object)]
        else:
            self.members = [_java_type(member) for member in _members(_unalias(annotation))]
        self.optional = any(java == NULL for java, _ in self.members)

    def check(self, function, index, argument):
        """Refuses an argument the annotation does not allow.

        Behavior: `PyCallableWrapperJpyImpl#verifyArguments`
        """
        javas = [java for java, _ in self.members]
        if javas == [None]:
            raise _Mismatch(f"{function}: Unsupported type hint in signature for argument {index}")
        known = [java for java in javas if java is not None]
        if argument in known or OBJECT in known:
            return
        if any(java in _JAVA_WIDENS.get(argument, ()) for java in known):
            return
        got = argument if argument == NULL or argument in _NULLS else f"class {argument}"
        shown = ", ".join(java if java in _NULLS else f"class {java}" for java in known)
        raise _Mismatch(
            f"{function}: Expected argument ({self.name}) to be either one of [{shown}] or their "
            f"compatible ones, got {got}"
        )

    def converter(self, argument):
        """What turns an argument of the Java type `argument` into the value the function is
        called with; `None` when the value is passed as it is.

        Behavior: `deephaven._udf._ParsedParam#setup_arg_converter`
        """
        found = None
        for java, effective in self.members:
            if java is None or not (java == argument or java in _CONVERTER_WIDENS.get(argument, ())):
                continue
            if argument == NULL or effective is object or effective is str:
                found = None
                continue
            found = effective
            null = _NULLS.get(argument) if self.optional else None
            if null is not None:
                if effective is int or effective is float or effective is bool:
                    found = lambda value, null=null: None if value == null else value
                else:
                    found = lambda value, null=null, effective=effective: (
                        None if value == null else effective(value)
                    )
            if found is int or found is float or found is bool:
                # the value is one already
                found = None
            numpy = _numpy()
            if numpy is not None and isinstance(found, type) and issubclass(found, numpy.generic):
                # a later member may name a Python type, which converts faster
                continue
            break
        return found


def _in_long(value) -> bool:
    return -(2**63) <= value < 2**63


def _result_converter(function, java, optional, exact):
    """What turns the value a function returned into the cell: an `int` within 64 bits, a
    `float`, a `bool`, a `str` or `None`. `exact` says that the annotation is the Python type
    itself, for which Java does not convert.

    Behavior: `deephaven._udf._ParsedReturnAnnotation#setup_return_converter`, and for the
    values that pass through unconverted the two ways Java calls a function
    (`PyCallableWrapperJpyImpl#call`, `#vectorizedCallable`), which agree on the values
    accepted here
    """

    def unfit(value):
        # Java gives a value, a wrong value or a failure, depending on the arguments of the call
        return _Refused(
            f"{function} returned a Python {type(value).__name__} for a {java} column",
            "io.deephaven.engine.util.PyCallableWrapperJpyImpl#call",
        )

    def long_of(value):
        if not _in_long(value):
            raise unfit(value)
        return value

    if java in _INTEGER_NULLS:
        if exact and not optional:
            def convert(value):
                if isinstance(value, int) and not isinstance(value, bool):
                    return long_of(value)
                raise unfit(value)
        else:
            def convert(value):
                if value is None and optional:
                    return None
                return long_of(int(value))
    elif java in _REAL_NULLS:
        if exact and not optional:
            def convert(value):
                if isinstance(value, float):
                    return value
                if isinstance(value, int) and not isinstance(value, bool):
                    return float(long_of(value))
                raise unfit(value)
        else:
            def convert(value):
                if value is None and optional:
                    return None
                return float(value)
    elif java == BOOLEAN:
        if optional:
            def convert(value):
                return None if value is None else bool(value)
        elif exact:
            def convert(value):
                if value is None or isinstance(value, bool):
                    return value
                raise unfit(value)
        else:
            convert = bool
    else:
        def convert(value):
            if value is None or isinstance(value, str):
                return value
            raise unfit(value)
    return convert


def _result(function, annotation):
    """The Java type of the column a function's return annotation gives, and the converter of
    its values.

    Behavior: `deephaven._udf#_parse_return_annotation`, `#_udf_parser`
    """
    no_type = _Refused(
        f"{function} has no return annotation that names a column type, and its values would be "
        "a column of org.jpy.PyObject",
        "io.deephaven.engine.util.PyCallableWrapperJpyImpl#parseSignature",
    )
    if annotation is inspect.Signature.empty:
        raise no_type
    annotation = _unalias(annotation)
    exact = annotation
    optional = False
    members = _members(annotation)
    if len(members) == 2 and type(None) in members:
        optional = True
        annotation = members[0] if members[1] is type(None) else members[1]
    java, _ = _java_type(annotation)
    if java == ARRAY:
        raise _Refused(f"{function} returns an array", "deephaven._udf#_udf_parser")
    if java == INSTANT:
        raise _Refused(
            f"{function} returns a time",
            "deephaven._udf._ParsedReturnAnnotation#setup_return_converter",
        )
    if java == "char":
        raise _Refused(
            f"{function} returns a char",
            "deephaven._udf._ParsedReturnAnnotation#setup_return_converter",
        )
    if java in (None, NULL, OBJECT):
        raise no_type
    return java, _result_converter(function, java, optional, _builtin(exact) is not None)


def _vectorized(function) -> bool:
    """Whether Java would call the function with arrays: a numba function or a numpy ufunc."""
    module = type(function).__module__ or ""
    if module == "numba" or module.startswith("numba."):
        return True
    numpy = _numpy()
    return numpy is not None and isinstance(function, numpy.ufunc)


def _prepare(function, name, arguments, operation):
    if _vectorized(function):
        raise _Refused(
            f"{name} is a numba function or a numpy ufunc",
            "io.deephaven.engine.util.PyCallableWrapperJpyImpl#prepareSignature",
        )
    for argument in arguments:
        if argument == INSTANT:
            raise _Refused(
                f"an Instant as an argument of {name}",
                "deephaven._udf._ParsedParam#_setup_datetime_arg_converter",
            )
        if argument != NULL and argument != BOOLEAN and argument != STRING and argument not in _NULLS:
            raise _Refused(f"{argument} as an argument of {name}", "deephaven._udf#_udf_parser")
    try:
        signature = inspect.signature(function, eval_str=True)
    except ValueError:
        # a builtin without a signature: Java calls it and takes whatever it returns
        signature = None
    java, convert = _result(name, signature.return_annotation if signature else inspect.Signature.empty)
    display = getattr(function, "__name__", None) or name
    parameters = [_Parameter(key, parameter.annotation) for key, parameter in signature.parameters.items()]
    if not parameters and arguments:
        raise _Mismatch(f"{display}: Expected no arguments, got {len(arguments)}")
    variadic = any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in signature.parameters.values())
    if variadic and not arguments:
        # Java's parser fails on the call: "Expression changed!"
        raise _Refused(
            f"a call of {name}, which takes *args, without arguments",
            "io.deephaven.engine.table.impl.lang.QueryLanguageParser#visit(MethodCallExpr)",
        )
    converters = []
    for index, argument in enumerate(arguments):
        position = min(index, len(parameters) - 1)
        parameter = parameters[position]
        parameter.check(display, index, argument)
        converters.append(parameter.converter(argument))
        if parameter.optional and operation is not None:
            # Java keeps one converter per parameter for all the calls of an operation, so
            # the call compiled last decides which value is null for every one of them
            first = operation.setdefault((id(function), position), (argument, function))[0]
            if first != argument:
                raise _Refused(
                    f"calls of {name} in one operation that give its parameter {parameter.name}, "
                    f"which may be None, a {first} and a {argument}",
                    "deephaven._udf._ParsedParam#setup_arg_converter",
                )
    return java, _runner(function, converters, convert)


def _runner(function, converters, convert):
    """The function that calls `function` for each row of a chunk: `run(rows, columns)` with one
    list per argument, each `rows` long, gives the list of the cells."""
    if not converters:
        def run(rows, columns):
            return [convert(function()) for _ in range(rows)]
    elif not any(converters):
        def run(rows, columns):
            return [convert(function(*row)) for row in zip(*columns)]
    else:
        def run(rows, columns):
            cells = []
            for row in zip(*columns):
                given = [c(value) if c else value for c, value in zip(converters, row)]
                cells.append(convert(function(*given)))
            return cells
    return run


def prepare(function, name, arguments, operation):
    """What the call `name(arguments)` of the callable `function` is: `("ok", Java type of the
    result, run)`, `("unsupported", why)` for a call Java makes and this server does not, or
    `("type", why)` for one Java refuses. `arguments` are the Java types of the arguments,
    `"null"` for the null literal. `operation` is a dict the calls of one table operation
    share, or `None`."""
    try:
        java, run = _prepare(function, name, list(arguments), operation)
    except _Refused as refused:
        return ("unsupported", str(refused))
    except _Mismatch as mismatch:
        return ("type", str(mismatch))
    except Exception as error:
        return ("type", f"the signature of {name} cannot be read: {type(error).__name__}: {error}")
    return ("ok", java, run)
