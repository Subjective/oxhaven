"""`io.deephaven.engine.table.impl.util.ColumnsToRowsTransform.columnsToRows`, the static
method that `jpy.get_type` hands a script of the reference (`deephaven.plot.express`'s
`dx.data.marketing` calls it). The engine carries it out (`oxhaven_engine::ops::columns_to_rows`).

The method has three overloads, and jpy chooses one by the Python values a script passes.
`columnsToRows` makes the same choice for the cases seen on the reference:

- `(Table, String, String, String...)`: three arguments or more. The values after the
  third are the columns, each a `str` or `None`; a single list or tuple of `str`, or
  `None`, is the whole array.
- `(Table, String, String, String[], String[])` and
  `(Table, String, String[], String[], String[][])`: five arguments. jpy prefers these to
  the first, and of the two the one whose arguments fit better; a tie, as with three
  `None`, is jpy's "ambiguous Java method call". A `str` fits a `String`; a list or tuple
  of `str` fits a `String[]`, and so does a `str`, as an array of its characters; any
  list or tuple fits a `String[][]`, whose items are arrays of `str` or `None`, and so
  does a `str`, as an array of one-character arrays. `None` fits any of them.

The table is a table handle (`Table.j_table`). Java's `null`s give the
`NullPointerException`s Java throws, in Java's order: those of the arguments before the
engine runs, and that of a `null` column name once the result is made.

Behavior: `io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows`, all
three overloads, and jpy 2.2.0's choice among them
"""

from deephaven._java_time import JavaError

_NO_MATCH = "no matching Java method overloads found"
_AMBIGUOUS = "ambiguous Java method call, too many matching method overloads found"
_NPE = "java.lang.NullPointerException: "


def _native():
    import _oxhaven

    return _oxhaven


def _is_handle(value) -> bool:
    return isinstance(value, _native().TableHandle)


# How well a value fits a parameter (0: not at all), and the value Java receives. The
# numbers only order the fits; jpy's are its own.


def _table(value):
    if value is None:
        return 1, None
    return (100, value) if _is_handle(value) else (0, None)


def _string(value):
    if value is None:
        return 1, None
    return (100, value) if isinstance(value, str) else (0, None)


def _strings(value):
    if value is None:
        return 1, None
    if isinstance(value, str):
        return 10, list(value)
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return 100, list(value)
    return 0, None


def _string_arrays(value):
    if value is None:
        return 1, None
    if isinstance(value, str):
        return 5, list(value)
    if isinstance(value, (list, tuple)):
        return 50, value
    return 0, None


def _converted_arrays(value):
    """The `String[][]` of a list or tuple that fit one: jpy converts the items only now."""
    arrays = []
    for item in value:
        if item is None:
            arrays.append(None)
        elif isinstance(item, str):
            arrays.append(list(item))
        elif isinstance(item, (list, tuple)):
            for element in item:
                if element is not None and not isinstance(element, str):
                    raise ValueError(
                        f"cannot convert a Python '{type(element).__name__}' to a Java 'java.lang.String'"
                    )
            arrays.append(list(item))
        else:
            raise ValueError(
                f"cannot convert a Python '{type(item).__name__}' to a Java array of type 'java.lang.String'"
            )
    return arrays


def _fit(kinds, args):
    """The sum of the fits of `args` to the parameter kinds, or 0, and the values."""
    total, values = 0, []
    for kind, arg in zip(kinds, args):
        fit, value = kind(arg)
        if fit == 0:
            return 0, None
        total += fit
        values.append(value)
    return total, values


def _varargs(args):
    """The arguments of `(Table, String, String, String...)`, or None if they do not fit."""
    fit, values = _fit((_table, _string, _string), args[:3])
    if fit == 0:
        return None
    rest = args[3:]
    if len(rest) == 1 and not isinstance(rest[0], str):
        fit, columns = _strings(rest[0])
        if fit == 0:
            return None
    elif all(arg is None or isinstance(arg, str) for arg in rest):
        columns = list(rest)
    else:
        return None
    return values + [columns]


def columnsToRows(*args):
    """columnsToRows(Table source, String labelColumn, String valueColumn, String... transposeColumns)
    columnsToRows(Table source, String labelColumn, String valueColumn, String[] labels, String[] transposeColumns)
    columnsToRows(Table source, String labelColumn, String[] valueColumns, String[] labels, String[][] transposeColumns)

    Convert value columns to labeled rows: the table handle of the result.
    """
    if len(args) == 5:
        single = _fit((_table, _string, _string, _strings, _strings), args)
        multiple = _fit((_table, _string, _strings, _strings, _string_arrays), args)
        if single[0] == 0 and multiple[0] == 0:
            raise JavaError(_NO_MATCH)
        if single[0] == multiple[0]:
            raise JavaError(_AMBIGUOUS)
        if single[0] > multiple[0]:
            source, label, value, labels, columns = single[1]
            return _transform(source, label, [value], labels, [columns])
        source, label, values, labels, groups = multiple[1]
        return _transform(source, label, values, labels, None if groups is None else _converted_arrays(groups))
    chosen = _varargs(args) if len(args) >= 3 else None
    if chosen is None:
        raise JavaError(_NO_MATCH)
    source, label, value, columns = chosen
    # the column names are the labels
    return _transform(source, label, [value], columns, [columns])


def _transform(source, label, values, labels, groups):
    """The checks of Java's arguments that come before the engine's, in Java's order, then
    the engine's transform."""
    if source is None:
        raise JavaError(
            _NPE + 'Cannot invoke "io.deephaven.engine.table.Table.coalesce()" because "source" is null'
        )
    if values is None:
        raise JavaError(_NPE + 'Cannot read the array length because "valueColumns" is null')
    if not values:
        raise JavaError("java.lang.IllegalArgumentException: No columns to transpose defined!")
    if groups is None:
        raise JavaError(_NPE + 'Cannot read the array length because "transposeColumns" is null')
    if len(values) != len(groups):
        raise JavaError(
            "java.lang.IllegalArgumentException: Inconsistent transpose column definition, "
            f"{len(values)} names defined, {len(groups)} columns defined."
        )
    for value, group in zip(values, groups):
        if labels is None:
            raise JavaError(_NPE + 'Cannot read the array length because "labels" is null')
        if group is None:
            raise JavaError(_NPE + 'Cannot read the array length because "transposeColumns[cc]" is null')
        if len(labels) != len(group):
            raise JavaError(
                f"java.lang.IllegalArgumentException: {len(labels)} labels defined, but "
                f"{len(group)} transpose columns defined for {'null' if value is None else value}."
            )
    if labels:
        # Java looks the columns up one by one: a `null` name is not found
        available = [definition[0] for definition in _native().column_definitions(source)]
        for group in groups:
            for name in group:
                if name is None or name not in available:
                    raise JavaError(
                        "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names "
                        f"[{'null' if name is None else name}], available column names are "
                        f"[{', '.join(available)}]"
                    )
    # a `null` column name fails only once the result is made; until then it stands for a
    # name no column has
    names = [label] + list(values)
    stand_ins = [f"\0{index}" if name is None else name for index, name in enumerate(names)]
    result = _native().columns_to_rows(source, stand_ins[0], stand_ins[1:], labels, groups)
    if None in names:
        raise JavaError(_NPE + "Column names cannot be null")
    return result
