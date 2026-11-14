# Application-mode script. The tables are set as fields of the application `ide_app`, so the
# server lists them with tickets `a/ide_app/f/<name>`. They are kept out of the script's scope:
# a table left there would be listed a second time, as the query scope variable `s/<name>`.
def _ide_app():
    import os
    import sys

    from deephaven.appmode import get_app_state

    sys.path.insert(0, os.environ["OXHAVEN_IDE_APP_DIR"])
    import ide_tables

    app = get_app_state()
    app["ide_static"], app["ide_ticking"] = ide_tables.make()


_ide_app()
del _ide_app
