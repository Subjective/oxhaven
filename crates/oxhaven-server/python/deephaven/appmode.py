"""Application mode: a script that starts an application sets the application's fields here.

Behavior: `deephaven.appmode` of the reference package (`py/server/deephaven/appmode.py`).
"""

import _oxhaven

from deephaven.dherror import DHError


class ApplicationState:
    """The state of one application: its fields by name.

    `app[name] = value` sets a field, `app[name]` reads one and `del app[name]` removes
    one. A name is made a string with `str()`.

    Behavior: `deephaven.appmode.ApplicationState`
    """

    def __init__(self, j_app_state):
        self.j_app_state = j_app_state

    @property
    def j_object(self):
        return self.j_app_state

    def __repr__(self):
        return f"id: {self.j_app_state.id()}, name: {self.j_app_state.name()}"

    def __str__(self):
        return repr(self)

    def __getitem__(self, item):
        item = str(item)
        field = self.j_app_state.getField(item)
        if not field:
            raise KeyError(item)
        return field[0]

    def __setitem__(self, key, value):
        self.j_app_state.setField(str(key), value)

    def __delitem__(self, key):
        # As in the reference: the field is removed, and then KeyError is raised whether
        # there was a field or not, because Java's removeField returns nothing and the
        # reference raises when what it returned is not true. Observed on Java 42.9.
        key = str(key)
        value = self.j_app_state.removeField(key)
        if not value:
            raise KeyError(key)

    @property
    def fields(self):
        """The fields as a dict of name to value, in the order of the names. The reference's
        dict is in the order of Java's hash table."""
        state = self.j_app_state
        found = ((name, state.getField(name)) for name in state.listFields())
        return {name: field[0] for name, field in found if field}


def get_app_state():
    """The state of the application whose script is running.

    Raises:
        DHError: when called by anything but a script that starts an application, while
            it runs and on the thread it runs on
    """
    try:
        return ApplicationState(j_app_state=_oxhaven.app_state())
    except Exception as e:
        raise DHError(e, "failed to get the application state.") from e
