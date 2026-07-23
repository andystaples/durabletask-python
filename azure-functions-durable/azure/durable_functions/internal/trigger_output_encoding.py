# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Force durable orchestration/entity trigger outputs to encode as ``string``.

The Durable Functions worker returns the base64-encoded ``OrchestratorResponse``
/ ``EntityBatchResult`` protobuf produced by durabletask (see
:mod:`azure.durable_functions.worker`). The Durable Functions host expects that
payload verbatim as a ``string`` Datum, which it base64-decodes back into the
gRPC message.

Some ``azure-functions`` releases register durable trigger converters whose
``encode`` emits a ``json`` Datum instead of a ``string`` one (this was the
behavior of the classic, pre-gRPC programming model). When that happens the host
tries to JSON-parse the base64 payload and the orchestration fails with a
deserialization error before any user code result is honored.

To stay decoupled from the ``azure-functions`` release cycle, we override the
orchestration and entity trigger converters in the azure-functions binding
registry so their output is always a ``string`` Datum, regardless of which
``azure-functions`` version is installed. The activity trigger converter is left
untouched because activity outputs are genuine user values that must round-trip
as JSON.
"""

from typing import Any, Optional

from azure.functions import meta
from azure.functions.durable_functions import (
    EnitityTriggerConverter as _EntityTriggerConverter,
    OrchestrationTriggerConverter as _OrchestrationTriggerConverter,
)

from ..constants import ENTITY_TRIGGER, ORCHESTRATION_TRIGGER


class _StringOutputOrchestrationTriggerConverter(
        _OrchestrationTriggerConverter, binding=None, trigger=True):
    """Orchestration trigger converter that encodes its output as ``string``."""

    @classmethod
    def encode(cls, obj: Any, *, expected_type: Optional[type]) -> meta.Datum:
        return meta.Datum(type='string', value=obj)


class _StringOutputEntityTriggerConverter(
        _EntityTriggerConverter, binding=None, trigger=True):
    """Entity trigger converter that encodes its output as ``string``."""

    @classmethod
    def encode(cls, obj: Any, *, expected_type: Optional[type]) -> meta.Datum:
        return meta.Datum(type='string', value=obj)


def ensure_string_trigger_output_encoding() -> None:
    """Override the durable trigger converters to emit ``string`` outputs.

    Idempotent: replaces the ``orchestrationTrigger`` and ``entityTrigger``
    entries in the azure-functions converter registry with the string-encoding
    variants above.
    """
    bindings = meta._ConverterMeta._bindings  # pyright: ignore[reportPrivateUsage]
    bindings[ORCHESTRATION_TRIGGER] = _StringOutputOrchestrationTriggerConverter
    bindings[ENTITY_TRIGGER] = _StringOutputEntityTriggerConverter
