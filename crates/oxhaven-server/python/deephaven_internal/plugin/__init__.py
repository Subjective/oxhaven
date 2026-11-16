"""What the server calls when it starts, to find the plugins of the environment.

Behavior: `deephaven_internal.plugin` of the reference, which Java's
`io.deephaven.server.plugin.python.PythonPluginsRegistration#registerInto` calls
"""

import _oxhaven


def initialize_all_and_register_into() -> None:
    """Registers the plugins of the server's own package, then those of every registration
    class that a package of the environment names as an entry point. An exception of a
    registration class is not caught: the server then does not start, as the Java server
    does not."""
    import deephaven.plugin

    from .register import RegistrationAdapter

    callback = RegistrationAdapter()

    # The reference's `deephaven` package is a distribution with an entry point of its own,
    # for its pandas plugin. This server's package is no distribution, so its plugin is
    # registered here; and the entry point of a reference package that happens to be
    # installed names the same module, which must not be registered twice.
    from deephaven.pandasplugin import PandasPluginRegistration

    PandasPluginRegistration.register_into(callback)
    for entry_point in deephaven.plugin.collect_registration_entrypoints():
        if entry_point.value.partition(":")[0].strip() == "deephaven.pandasplugin":
            continue
        entry_point.load().register_into(callback)
    _oxhaven.log("INFO", f"Registered plugins: objectType={callback.object_types},js={callback.js_plugins}\n")
