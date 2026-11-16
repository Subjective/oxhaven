"""The plugin of the package itself: a pandas `DataFrame` in a variable of the console is
listed to clients, and opens as a table.

Behavior: `deephaven.pandasplugin` of the reference
"""

from deephaven.plugin import Callback, Registration

from . import pandas_as_table


class PandasPluginRegistration(Registration):
    @classmethod
    def register_into(cls, callback: Callback) -> None:
        callback.register(pandas_as_table.PandasDataFrameSerializer)
