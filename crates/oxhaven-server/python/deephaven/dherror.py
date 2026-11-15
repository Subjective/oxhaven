"""The exception of the Deephaven Python package."""


class DHError(Exception):
    """Raised when a Deephaven operation fails.

    `cause` is the error underneath, if there is one, and `message` says which operation
    failed. As in the reference package, `DHError("text")` is a message without a cause.

    Behavior: `deephaven.dherror.DHError` (the names and the text of `str()`; the parsing of
    Java stack traces has no counterpart here)
    """

    def __init__(self, cause=None, message=""):
        if isinstance(cause, str) and message == "":
            message, cause = cause, None
        self._message = message
        self._cause = cause
        self._root_cause = str(cause) if cause is not None else message
        super().__init__(message, cause)

    @property
    def root_cause(self):
        """The innermost error text."""
        return self._root_cause

    @property
    def compact_traceback(self) -> str:
        """The error text without stack frames."""
        return self._root_cause

    def __str__(self):
        if self._cause is None:
            return self._message
        return f"{self._message} : {self._cause}"
