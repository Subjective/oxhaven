"""Plugins: how a Python package adds behaviour to the server.

A package names a `Registration` class under the entry point group `deephaven.plugin`, with
the entry point name `registration_cls`. When the server starts it loads every such class
and calls its `register_into` with a `Callback`, which the class hands its plugins to.

This module has the names and signatures of the `deephaven-plugin` package (0.8.0), which
plugins are written against, and none of its code. It is part of the server, so the
`deephaven-plugin` package of the environment, if there is one, is not used.
"""

import abc
from typing import List, Type, Union

__version__ = "0.8.0"

DEEPHAVEN_PLUGIN_ENTRY_KEY = "deephaven.plugin"
DEEPHAVEN_PLUGIN_REGISTRATION_CLASS = "registration_cls"


class Plugin(abc.ABC):
    """Something a package adds to the server: an object type or a JS plugin."""


class Callback(abc.ABC):
    """What `Registration.register_into` is given to hand its plugins to."""

    @abc.abstractmethod
    def register(self, plugin: Union[Plugin, Type[Plugin]]) -> None:
        """Registers a plugin, or a plugin class, which is then created without arguments."""


class Registration(abc.ABC):
    """What a package names as its entry point: the class that registers its plugins."""

    @classmethod
    @abc.abstractmethod
    def register_into(cls, callback: Callback) -> None:
        """Calls `callback.register` once for each plugin of the package."""

    @classmethod
    def collect_plugins(cls) -> List[Union[Plugin, Type[Plugin]]]:
        """The plugins this class registers, as it hands them over."""
        collected: List[Union[Plugin, Type[Plugin]]] = []

        class _Collector(Callback):
            def register(self, plugin: Union[Plugin, Type[Plugin]]) -> None:
                collected.append(plugin)

        cls.register_into(_Collector())
        return collected


def collect_registration_entrypoints():
    """The entry points of the environment that name a registration class."""
    from importlib.metadata import entry_points

    found = entry_points(group=DEEPHAVEN_PLUGIN_ENTRY_KEY, name=DEEPHAVEN_PLUGIN_REGISTRATION_CLASS)
    return found or []


def collect_registration_classes():
    """The registration classes of the environment, loaded."""
    return [entry_point.load() for entry_point in collect_registration_entrypoints()]


def register_all_into(callback: Callback) -> None:
    """Lets every registration class of the environment register its plugins."""
    for registration_cls in collect_registration_classes():
        registration_cls.register_into(callback)


def list_registrations() -> None:
    """Prints the registration classes of the environment."""
    lines = ["Listing Deephaven Plugin registrations:"]
    lines.extend(f"  {registration_cls}" for registration_cls in collect_registration_classes())
    print("\n".join(lines))


def list_plugins() -> None:
    """Prints the registration classes of the environment and the plugins of each."""
    lines = ["Listing Deephaven Plugins:"]
    for registration_cls in collect_registration_classes():
        lines.append(f"  {registration_cls}")
        lines.extend(f"    {plugin}" for plugin in registration_cls.collect_plugins())
    print("\n".join(lines))


def list_registrations_console() -> None:
    """What the command `deephaven-plugin-list-registrations` runs."""
    list_registrations()


def list_plugins_console() -> None:
    """What the command `deephaven-plugin-list-plugins` runs."""
    list_plugins()
