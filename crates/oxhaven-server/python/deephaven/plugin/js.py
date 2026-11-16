"""JS plugins: JavaScript that a package adds to the web IDE.

The class is here so that a package that has one can be imported and can register its
other plugins. This server does not serve JS plugins yet: one that is registered is left
out, and the server's log says so (`docs/OBJECTS.md`).

The names and signatures are those of `deephaven.plugin.js` in the `deephaven-plugin`
package (0.8.0).
"""

import abc
import pathlib
import typing

from . import Plugin

JsonValue = typing.Union[
    None,
    bool,
    int,
    float,
    str,
    typing.List["JsonValue"],
    typing.Dict[str, "JsonValue"],
]
"""Any value that JSON can hold."""


class JsPlugin(Plugin):
    """A directory of files for the web IDE, served under `js-plugins/{name}/` and listed
    in `js-plugins/manifest.json`."""

    @abc.abstractmethod
    def path(self) -> pathlib.Path:
        """The directory of the files to serve."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of the plugin, which is also its place under `js-plugins/`."""

    @property
    @abc.abstractmethod
    def version(self) -> str:
        """The version of the plugin, as the manifest gives it."""

    @property
    @abc.abstractmethod
    def main(self) -> str:
        """The main JavaScript file, relative to `path()`."""

    @property
    def loader(self) -> typing.Optional[JsonValue]:
        """The loader configuration for the manifest, if the plugin has one."""
        return None

    def __str__(self) -> str:
        return f"{self.name}@{self.version}"
