# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Process-wide payload storage configured at Function app startup."""

import json

from google.protobuf.wrappers_pb2 import StringValue

from durabletask.internal.orchestrator_service_pb2 import ActivityRequest, ActivityResponse
from durabletask.payload import (
    LargePayloadStorageOptions,
    PayloadStore,
    deexternalize_payloads,
    externalize_payloads,
)

_payload_store: PayloadStore | None = None


def configure_payload_store(payload_store: object) -> None:
    """Register one store per worker process, allowing identical registrations."""
    global _payload_store
    if not isinstance(payload_store, PayloadStore):
        raise TypeError("payload_store must be a PayloadStore")
    if _payload_store is not None and _payload_store is not payload_store:
        raise ValueError("A different payload store is already configured in this process")
    _payload_store = payload_store


def get_payload_store() -> PayloadStore | None:
    """Return the app's store, or None when externalization is disabled."""
    return _payload_store


class _FunctionsPayloadStore(PayloadStore):
    """Keep references valid JSON for the Functions host's payload readers."""

    def __init__(self, store: PayloadStore) -> None:
        self._store = store

    @property
    def options(self) -> LargePayloadStorageOptions:
        return self._store.options

    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        return json.dumps(self._store.upload(data, instance_id=instance_id))

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        return json.dumps(await self._store.upload_async(data, instance_id=instance_id))

    def _unwrap(self, token: str) -> str:
        if self._store.is_known_token(token):
            return token
        try:
            value = json.loads(token)
        except (ValueError, TypeError):
            return token
        return value if isinstance(value, str) else token

    def is_known_token(self, value: str) -> bool:
        return self._store.is_known_token(self._unwrap(value))

    def download(self, token: str) -> bytes:
        return self._store.download(self._unwrap(token))

    async def download_async(self, token: str) -> bytes:
        return await self._store.download_async(self._unwrap(token))


def get_transport_payload_store() -> PayloadStore | None:
    """Resolve the configured store with Functions-compatible reference encoding."""
    store = get_payload_store()
    return _FunctionsPayloadStore(store) if store is not None else None


def deexternalize_payload(value: str) -> str:
    """Resolve a reference before the Functions JSON codec reads the payload."""
    store = get_transport_payload_store()
    if store is None:
        return value
    request = ActivityRequest(input=StringValue(value=value))
    deexternalize_payloads(request, store)
    return request.input.value


def externalize_activity_output(value: str) -> str:
    """Apply the core payload policy to a serialized activity output."""
    store = get_transport_payload_store()
    if store is None:
        return value
    response = ActivityResponse(result=StringValue(value=value))
    externalize_payloads(response, store)
    return response.result.value
