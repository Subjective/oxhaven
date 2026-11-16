"""The object type of a pandas `DataFrame`: no bytes, and a reference to the table made
from the frame.

The reference imports pandas with this module, which it can: its package needs pandas.
This server runs without pandas too, so pandas is looked for only among the modules that
are loaded: an object cannot be a `DataFrame` before pandas is.

Behavior: `deephaven.pandasplugin.pandas_as_table` of the reference
"""

import sys

from deephaven.plugin.object_type import Exporter, FetchOnlyObjectType

NAME = "pandas.DataFrame"


class PandasDataFrameSerializer(FetchOnlyObjectType):
    @property
    def name(self) -> str:
        return NAME

    def is_type(self, object) -> bool:
        pandas = sys.modules.get("pandas")
        frame = getattr(pandas, "DataFrame", None)
        return isinstance(frame, type) and isinstance(object, frame)

    def to_bytes(self, exporter: Exporter, data_frame) -> bytes:
        from deephaven.pandas import to_table

        exporter.reference(to_table(data_frame))
        return b""
