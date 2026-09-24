# Azure Functions Durable (Python) — 2.x

`azure-functions-durable` is the Python SDK provider for
[Durable Azure Functions](https://learn.microsoft.com/azure/azure-functions/durable/),
built on top of the [`durabletask`](https://pypi.org/project/durabletask/) SDK.

> [!NOTE]
> 2.x is a ground-up rewrite of the Durable Functions Python SDK on top of the
> `durabletask` runtime. It is currently a preview (beta) release; APIs may
> change before the stable 2.0.0.

## Requirements

- Python 3.13+
- The decorator-based Azure Functions programming model (`DFApp` / `Blueprint`)

## Installation

```bash
pip install azure-functions-durable
```

## Overview

Author orchestrations, activities, and entities as Azure Functions and let the
Durable Task runtime handle scheduling, checkpointing, and replay. Both
durabletask-native two-argument functions (`def orchestrator(ctx, input)`) and
v1-style single-argument functions (`def orchestrator(context)`) are supported,
along with class-based entities and a compatibility layer over the v1 API.

Key capabilities include durable orchestrations and sub-orchestrations, durable
timers, external events, durable entities, retries, versioning, durable HTTP
calls (`context.call_http(...)`), recurring scheduled tasks, and history export.

## Large payloads

Configure a `durabletask.payload.PayloadStore` once at app startup to store large
serialized payloads outside orchestration history. For Azure Blob Storage, install
the optional dependencies:

```bash
pip install azure-functions-durable "durabletask[azure-blob-payloads]" aiohttp
```

In your Function app, configure the root `DFApp` before any invocations:

```python
import os

import azure.durable_functions as df
from durabletask.extensions.azure_blob_payloads import (
    BlobPayloadStore,
    BlobPayloadStoreOptions,
)

app = df.DFApp()
app.configure_large_payloads(
    payload_store=BlobPayloadStore(BlobPayloadStoreOptions(
        connection_string=os.environ["PAYLOAD_STORAGE_CONNECTION_STRING"],
        container_name="durable-payloads",
        threshold_bytes=256 * 1024,
    ))
)
```

Set `PAYLOAD_STORAGE_CONNECTION_STRING` in your Function app settings (or in
`local.settings.json` for local development). The store automatically uploads
serialized payloads above the threshold and downloads their contents when the
SDK consumes them. Orchestration and activity inputs and outputs, custom status,
external events, and entity inputs, results, and state use the configured store.
Sub-orchestrations and continue-as-new use it as well. The default maximum stored
payload size is 10 MiB; `max_stored_payload_bytes` can configure this limit.

Configuration applies to both synchronous and asynchronous durable clients and
all registered blueprints, including blueprints imported before configuration.
There is one store per Python worker process. Registering the same store object
again is allowed; registering a different object raises `ValueError`. Configure
every scaled-out worker with access to the same backing storage and retain that
access across deployments. Keep the store open for the process lifetime.

> [!WARNING]
> Keep payload blobs for as long as any retained orchestration history or entity
> state references them, including histories needed for replay. Purging an
> orchestration does not delete its payload blobs; manage retention separately.

This is SDK-managed storage, separate from the Azure Storage backend's automatic
large-message handling. Without configuration, existing behavior is unchanged.
Use the configured Python clients to retrieve hydrated payloads. Host management
HTTP endpoints and other consumers that do not use this configuration can expose
reference strings instead. Applications exchanging externalized payloads must
agree on the store and reference encoding; Functions references are JSON strings.

## Unit testing entities

Use `execute_entity()` to run one entity operation in-process without a
Functions host or Durable Task backend. It supports v1-style entity functions,
durabletask-native entity functions, and `DurableEntity` subclasses:

```python
from azure.durable_functions.testing import execute_entity
from durabletask.entities import DurableEntity


class Counter(DurableEntity):
    def add(self, amount: int) -> int:
        value = self.get_state(int, 0) + amount
        self.set_state(value)
        return value


outcome = execute_entity(Counter, "add", input=2, state=3)

assert outcome.get_result() == 5
assert outcome.get_state() == 5
assert outcome.actions == ()
```

For an `entity_trigger`-decorated function, pass the exposed entity function:

```python
import azure.durable_functions as df
from azure.durable_functions.testing import execute_entity


app = df.DFApp()


@app.entity_trigger(context_name="context")
def counter(context: df.DurableEntityContext) -> None:
    value = context.get_state(initializer=lambda: 0)
    value += context.get_input()
    context.set_state(value)
    context.set_result(value)


entity_function = counter.build().get_user_function().entity_function
outcome = execute_entity(entity_function, "add", input=2, state=3)

assert outcome.get_result() == 5
assert outcome.get_state() == 5
```

The returned `EntityTestResult` provides `get_result()` and `get_state()`
methods plus typed signal or orchestration-start actions scheduled by the
operation. Pass `expected_type` when reconstructing a custom payload:

```python
assert outcome.get_state(expected_type=CounterState) == CounterState(value=5)
```

## Links

- [2.x samples](samples/)
- [Migration guide from 1.x](MIGRATION_GUIDE.md)
- [Changelog](CHANGELOG.md)
- [Durable Functions documentation](https://learn.microsoft.com/azure/azure-functions/durable/)
- [`durabletask` on PyPI](https://pypi.org/project/durabletask/)
- [Azure Functions Durable 1.x source](https://github.com/Azure/azure-functions-durable-python)
- [Azure Functions Python library](https://github.com/Azure/azure-functions-python-library)
- [Azure Functions Python worker](https://github.com/Azure/azure-functions-python-worker)
- [Repository](https://github.com/microsoft/durabletask-python)

## License

Licensed under the [MIT License](LICENSE).
