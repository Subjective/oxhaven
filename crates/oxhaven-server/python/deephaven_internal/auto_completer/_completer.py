"""The object the server asks for completions, signatures and hover texts.

Behavior: `py/server/deephaven_internal/auto_completer/_completer.py`. The names, what each
method takes and what it returns are the reference's, because the server calls them and a
script may; the code is this server's.
"""

from enum import Enum

from jedi import Interpreter, Script

from ._signature_help import get_signature_help


class Mode(Enum):
    OFF = "OFF"
    SAFE = "SAFE"
    STRONG = "STRONG"

    def __str__(self):
        return self.value


# The kinds of completion items of the language server protocol, by the type jedi gives a
# completion. A type that is not listed is `text`.
_ITEM_KINDS = {
    "text": 1,
    "function": 3,
    "instance": 6,
    "param": 6,
    "statement": 6,
    "class": 7,
    "module": 9,
    "property": 10,
    "keyword": 14,
    "path": 17,
}

# The reference lowers the interpreter's recursion limit to this before a completion on
# Python 3.9 and 3.10, where jedi could otherwise crash the process.
MAX_RECURSION_LIMIT = 2000


def _fenced(language, text):
    return f"```{language}\n{text}\n```" if text else ""


def wrap_python(txt: str) -> str:
    """`txt` as a block of Python code in Markdown; nothing for an empty text."""
    return _fenced("python", txt)


def wrap_plaintext(txt: str) -> str:
    """`txt` as a block of plain text in Markdown; nothing for an empty text."""
    return _fenced("plaintext", txt)


class Completer:
    """Holds the documents clients have open, by URI, and answers requests about them.

    A request names the version of the document it was made for. One for a version that is
    no longer the newest is answered with nothing, and one that is overtaken while it is
    being answered gives up.
    """

    def __init__(self):
        self._docs = {}
        self._versions = {}
        # replaced by the session's globals when a client opens a stream
        self.__scope = globals()
        try:
            import jedi  # noqa: F401

            self.__can_jedi = True
            self.mode = Mode.STRONG
        except ImportError:
            self.__can_jedi = False
            self.mode = Mode.OFF

    @property
    def mode(self) -> Mode:
        return self.__mode

    @mode.setter
    def mode(self, mode) -> None:
        # the name of a mode will do
        self.__mode = Mode[mode] if type(mode) is str else mode

    def open_doc(self, text: str, uri: str, version: int) -> None:
        self._docs[uri] = text
        self._versions[uri] = version

    def get_doc(self, uri: str) -> str:
        return self._docs[uri]

    def update_doc(self, text: str, uri: str, version: int) -> None:
        self._docs[uri] = text
        self._versions[uri] = version

    def close_doc(self, uri: str) -> None:
        del self._docs[uri]
        del self._versions[uri]

    def is_enabled(self) -> bool:
        return self.__mode != Mode.OFF

    def can_jedi(self) -> bool:
        return self.__can_jedi

    def set_scope(self, scope: dict) -> None:
        self.__scope = scope

    def get_completer(self, uri: str):
        text = self.get_doc(uri)
        if self.__mode == Mode.SAFE:
            return Script(text)
        return Interpreter(text, [self.__scope])

    def _is_newest(self, uri, version):
        return self._versions[uri] == version

    def do_completion(self, uri: str, version: int, line: int, col: int) -> list:
        """The completions at a position, each as `[name, column where the name starts,
        detail, docstring, kind]`: public names first, then names that start with one
        underscore, then those that start with two. `line` counts from 1."""
        self.check_recursion_limit()
        if not self._is_newest(uri, version):
            return []
        public, private, dunder = [], [], []
        for completion in self.get_completer(uri).complete(line, col):
            # completing file names in strings is of no use to a console
            if completion.type == "path":
                continue
            if not self._is_newest(uri, version):
                return []
            result = self.to_completion_result(completion, col)
            name = result[0]
            if name.startswith("__"):
                dunder.append(result)
            elif name.startswith("_"):
                private.append(result)
            else:
                public.append(result)
        return public + private + dunder

    @staticmethod
    def to_completion_result(completion, col: int) -> list:
        signatures = completion.get_signatures()
        return [
            completion.name,
            col - completion.get_completion_prefix_length(),
            signatures[0].to_string() if signatures else completion.description,
            completion.docstring(raw=True),
            _ITEM_KINDS.get(completion.type, _ITEM_KINDS["text"]),
        ]

    def do_signature_help(self, uri: str, version: int, line: int, col: int) -> list:
        """The signatures of the call the position is in, each as `[label, documentation,
        [[parameter label, parameter documentation], ...], index of the parameter the
        position is at]`."""
        if not self._is_newest(uri, version):
            return []
        results = []
        for signature in self.get_completer(uri).get_signatures(line, col):
            if not self._is_newest(uri, version):
                return []
            results.append(get_signature_help(signature))
        return results

    def do_hover(self, uri: str, version: int, line: int, col: int) -> str:
        """Markdown about the name at a position: a header in a block of Python code and,
        under a rule, the docstring as plain text. Nothing for a keyword."""
        if not self._is_newest(uri, version):
            return ""
        found = self.get_completer(uri).help(line, col)
        if not found or found[0].type == "keyword":
            return ""
        name = found[0]
        kind = name.type
        signatures = name.get_signatures()
        if signatures:
            header = f"{'def' if kind == 'function' else kind} {signatures[0].to_string()}"
        elif kind == "class":
            header = f"class {name.name}()"
        elif kind == "function":
            header = f"def {name.name}()"
        elif kind == "property":
            header = f"property {name.name}"
        else:
            header = name.description
        text = wrap_python(header)
        docstring = name.docstring(raw=True)
        if docstring:
            text += "\n---\n" + wrap_plaintext(docstring)
        return text.strip()

    def check_recursion_limit(self, suppress_warning: bool = False) -> None:
        """Does nothing: the reference lowers the recursion limit on Python 3.9 and 3.10
        only, and this server needs Python 3.11 (`docs/CONSOLE.md`)."""
