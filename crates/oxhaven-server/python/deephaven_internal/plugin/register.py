"""The callback that registration classes hand their plugins to.

Behavior: `deephaven_internal.plugin.register` of the reference, and
`io.deephaven.server.plugin.python.CallbackAdapter`
"""

from typing import Type, Union

import _oxhaven
from deephaven.plugin import Callback, Plugin
from deephaven.plugin.js import JsPlugin
from deephaven.plugin.object_type import ObjectType

from .js import register_js_plugin
from .object import ObjectTypeAdapter


class RegistrationAdapter(Callback):
    """Registers object types and JS plugins with the server."""

    def __init__(self):
        self.object_types = 0
        self.js_plugins = 0

    def register(self, plugin: Union[Plugin, Type[Plugin]]) -> None:
        if isinstance(plugin, type):
            plugin = plugin()
        if isinstance(plugin, ObjectType):
            _oxhaven.register_object_type(plugin.name, ObjectTypeAdapter(plugin))
            self.object_types += 1
        elif isinstance(plugin, JsPlugin):
            register_js_plugin(plugin)
            self.js_plugins += 1
        else:
            raise NotImplementedError(f"Unexpected type: {type(plugin)}")

    def __str__(self) -> str:
        return "the object types and JS plugins of the server"
