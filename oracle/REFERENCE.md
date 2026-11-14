# Reference implementation

The oracle is Deephaven Community Core **42.9**. Everything that defines "what Java does" comes
from this one release:

| Use | Pin |
| --- | --- |
| Running server (the oracle) | `deephaven-server==42.9` from PyPI, or `ghcr.io/deephaven/server:42.9` |
| Python client used by the harness | `pydeephaven==42.9` |
| Java source to port from and cite | `deephaven/deephaven-core` at tag `v42.9` |
| Protocol definitions | The `.proto` and Barrage `.fbs` files at tag `v42.9` |

The pin does not move during a run. Changing it is its own task and requires re-recording the
oracle corpus.
