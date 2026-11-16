"""Hands a JS plugin of a package to the server.

The reference converts the plugin to a Java object through jpy and Java checks it. The
checks are the server's here too; what jpy refused before Java saw the plugin is refused
here with jpy's texts, as Java 42.9 with jpy 2.2.0 was seen to answer (`docs/JS_PLUGINS.md`).

Behavior: `deephaven_internal.plugin.js.to_j_js_plugin` of the reference,
`deephaven.jcompat.j_json_value`, and
`io.deephaven.server.plugin.python.CallbackAdapter#registerJsPlugin`
"""

import decimal
import math
import pathlib

import _oxhaven
from deephaven.plugin.js import JsPlugin

_NO_OVERLOAD = "no matching Java method overloads found"
_AMBIGUOUS = "ambiguous Java method call, too many matching method overloads found"
_ESCAPES = {
    '"': '\\"',
    "\\": "\\\\",
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


def _string(text: str) -> str:
    """A string as the manifest has it: only the quote, the backslash and the control
    characters are escaped."""
    return (
        '"'
        + "".join(
            _ESCAPES.get(c) or (f"\\u{ord(c):04X}" if c < " " else c) for c in text
        )
        + '"'
    )


def _double(value: float) -> str:
    """A number as `java.lang.Double#toString` writes it. What is no number is a string
    in the manifest, as Java's JSON writer makes it."""
    if math.isnan(value):
        return '"NaN"'
    if math.isinf(value):
        return '"Infinity"' if value > 0 else '"-Infinity"'
    sign = "-" if math.copysign(1.0, value) < 0 else ""
    if value == 0:
        return sign + "0.0"
    shortest = repr(abs(value))
    # Java writes two digits at least, and then the two nearest the number: the smallest
    # double is `4.9E-324`, where the shortest text that reads back is `5e-324`
    two = f"{abs(value):.1e}"
    if float(two) == abs(value):
        mantissa, _, power = shortest.partition("e")
        if len(mantissa.replace(".", "").strip("0")) <= 1:
            shortest = two
    _, digit_tuple, power = decimal.Decimal(shortest).as_tuple()
    digits = "".join(map(str, digit_tuple)).rstrip("0") or "0"
    # the power of ten of the first digit
    exponent = len(digit_tuple) + power - 1
    if -3 <= exponent < 7:
        if exponent < 0:
            return f"{sign}0.{'0' * (-exponent - 1)}{digits}"
        whole = exponent + 1
        if len(digits) <= whole:
            return f"{sign}{digits}{'0' * (whole - len(digits))}.0"
        return f"{sign}{digits[:whole]}.{digits[whole:]}"
    return f"{sign}{digits[0]}.{digits[1:] or '0'}E{exponent}"


def _json(value, call: str) -> str:
    """The JSON of a loader configuration. `call` names the jpy call that hands the value
    to Java in the reference, which fails for an integer that is no Java `long`."""
    if isinstance(value, dict):
        put = "class='java.util.LinkedHashMap', name='put', methodCount=1"
        return (
            "{"
            + ",".join(f"{_key(k, put)}:{_json(v, put)}" for k, v in value.items())
            + "}"
        )
    if isinstance(value, (list, tuple)):
        add = "class='java.util.ArrayList', name='add', methodCount=2"
        return "[" + ",".join(_json(v, add) for v in value) + "]"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise SystemError(
                f"jpy.JOverloadedMethod({call}) returned a result with an exception set"
            )
        return str(value)
    if isinstance(value, float):
        return _double(value)
    if isinstance(value, str):
        return _string(value)
    # jpy hands any other object to Java as it is, and Java's JSON writer then fails or
    # writes the fields of a Java wrapper
    raise NotImplementedError(
        f"a loader value of {type(value)}: not ported: "
        "io.deephaven.server.plugin.js.JsPluginManifestEntry#loader"
    )


def _key(key, call: str) -> str:
    """The key of a JSON object: Java's JSON writer writes a key that is no string as its
    text."""
    if isinstance(key, str):
        return _string(key)
    text = _json(key, call)
    return text if text.startswith('"') else f'"{text}"'


def _text(value, null: str) -> str:
    """A value jpy hands to a Java method that takes a `String`."""
    if value is None:
        raise RuntimeError(null)
    if not isinstance(value, str):
        raise RuntimeError(_NO_OVERLOAD)
    return value


def register_js_plugin(js_plugin: JsPlugin) -> None:
    """Checks a JS plugin and has the server copy and list its files."""
    path = js_plugin.path()
    if not isinstance(path, pathlib.Path):
        raise Exception(
            f"Expecting pathlib.Path, is type(js_plugin.path())={type(path)}, js_plugin={js_plugin}"
        )
    main = _text(js_plugin.main, _AMBIGUOUS)
    name = _text(js_plugin.name, "java.lang.NullPointerException: name")
    version = _text(js_plugin.version, "java.lang.NullPointerException: version")
    # the loader is no required member of a JS plugin
    loader = getattr(js_plugin, "loader", None)
    if loader is not None:
        loader = _json(
            loader,
            "class='io.deephaven.plugin.js.JsPlugin$Builder', name='loader', methodCount=1",
        )
    _oxhaven.register_js_plugin(name, version, main, str(path), loader)
