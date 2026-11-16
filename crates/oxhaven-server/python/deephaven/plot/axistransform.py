"""Transforms of an axis, such as a logarithm or a business calendar.

Not ported: a transform reaches a client only as the business calendar of an axis, and
this server has no business calendars.

Behavior: `io.deephaven.plot.axistransformations.AxisTransforms`
"""


class AxisTransform:
    """A transform of the values along an axis."""

    def __init__(self, j_axis_transform=None):
        raise NotImplementedError(
            "not ported: io.deephaven.plot.axistransformations.AxisTransforms#axisTransform"
        )


def axis_transform_names() -> list:
    """The names of the transforms there are."""
    raise NotImplementedError(
        "not ported: io.deephaven.plot.axistransformations.AxisTransforms#axisTransformNames"
    )


def axis_transform(name: str) -> AxisTransform:
    """The transform of a name."""
    raise NotImplementedError(
        "not ported: io.deephaven.plot.axistransformations.AxisTransforms#axisTransform"
    )
