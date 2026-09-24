# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the durabletask-native activity adapter (``wrap_activity``)."""

from __future__ import annotations

import inspect
import asyncio
import json
import threading
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from azure.functions import meta
from azure.durable_functions.internal import payloads
from azure.durable_functions.internal.compat.activity import wrap_activity, wrap_activity_payloads
from azure.durable_functions.internal.converters import ActivityTriggerConverter


def test_one_param_activity_passes_through_unchanged():
    def act(x):
        return x

    assert wrap_activity(act, "x") is act


def test_two_param_activity_is_adapted_to_single_input():
    def act(ctx, payload):
        return (ctx, payload)

    adapted = wrap_activity(act, "payload")

    assert adapted is not act
    assert list(inspect.signature(adapted).parameters) == ["payload"]
    assert adapted.__name__ == "act"
    # A placeholder context is supplied; the input is passed through.
    ctx, payload = adapted("value")
    assert payload == "value"
    # Reading the placeholder context raises a clear error rather than an
    # opaque AttributeError on None.
    with pytest.raises(NotImplementedError, match="ActivityContext is not available"):
        _ = ctx.orchestration_id


def test_adapter_invokes_original_positionally_regardless_of_param_name():
    # The original's second parameter name differs from input_name; the adapter
    # still binds correctly because it calls the original positionally.
    def act(ctx, original_name):
        return original_name

    adapted = wrap_activity(act, "input")
    assert list(inspect.signature(adapted).parameters) == ["input"]
    assert adapted("hi") == "hi"


def test_adapter_sanitizes_parameterized_generic_annotations():
    def act(ctx, payload: Mapping[str, Any]) -> dict[str, Any]:
        return dict(payload)

    adapted = wrap_activity(act, "payload")
    # Parameterized generics (rejected by the worker indexer) are reduced to
    # concrete builtins.
    assert adapted.__annotations__ == {"payload": dict, "return": dict}


def test_native_activity_with_extra_binding_passes_through_unchanged():
    # A native Functions activity whose first positional parameter IS the
    # trigger input (== input_name) may declare additional host bindings (for
    # example a durable client) as further parameters. It must be left untouched
    # so the host binds each parameter by name, not adapted as a
    # durabletask-native ``(ctx, input)`` activity.
    def act(input, client):
        return (input, client)

    assert wrap_activity(act, "input") is act


def test_adapter_preserves_concrete_annotations():
    def act(ctx, name: str) -> str:
        return name

    adapted = wrap_activity(act, "name")
    assert adapted.__annotations__ == {"name": str, "return": str}


def test_adapter_without_annotations_has_none():
    def act(ctx, payload):
        return payload

    adapted = wrap_activity(act, "payload")
    assert adapted.__annotations__ == {}


def test_adapter_rejects_invalid_input_name():
    def act(ctx, payload):
        return payload

    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "not an identifier")
    with pytest.raises(ValueError, match="valid Python identifier"):
        wrap_activity(act, "class")  # a keyword


def test_activity_converters_do_not_access_storage(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("converter download")))
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("converter upload")))
    value = json.dumps("blob:v1:test-container:missing")
    decoded = ActivityTriggerConverter.decode(meta.Datum(type="json", value=value), trigger_metadata=None)
    assert isinstance(decoded, payloads.ActivityPayload)
    assert decoded.value == value
    encoded = ActivityTriggerConverter.encode({"large": "x" * 200}, expected_type=None)
    assert json.loads(encoded.value) == {"large": "x" * 200}
    assert ActivityTriggerConverter.encode(decoded, expected_type=None).value == value


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True])
async def test_async_activity_awaits_storage_and_preserves_signature(
        monkeypatch, payload_store_factory, native):
    store = payload_store_factory()
    entered = asyncio.Event()
    release = asyncio.Event()
    value = {"large": "x" * 200}
    token = store.upload(json.dumps(value).encode())
    upload = store.upload

    async def download_async(reference):
        entered.set()
        await release.wait()
        return store._blobs[reference]

    async def upload_async(data, *, instance_id=None):
        return upload(data, instance_id=instance_id)

    monkeypatch.setattr(store, "download_async", AsyncMock(side_effect=download_async))
    monkeypatch.setattr(store, "upload_async", AsyncMock(side_effect=upload_async))
    monkeypatch.setattr(store, "download", Mock(side_effect=AssertionError("sync download")))
    monkeypatch.setattr(store, "upload", Mock(side_effect=AssertionError("sync upload")))
    monkeypatch.setattr(payloads, "_payload_store", None)

    async def activity(payload, client):
        assert client == "extra binding"
        assert payload == value
        return payload

    async def native_activity(context, payload):
        assert payload == value
        return payload

    adapted = wrap_activity(native_activity if native else activity, "payload")
    wrapper = wrap_activity_payloads(adapted, "payload")
    assert inspect.iscoroutinefunction(wrapper)
    assert inspect.signature(wrapper) == inspect.signature(adapted)
    monkeypatch.setattr(payloads, "_payload_store", store)
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="json", value=json.dumps(token)), trigger_metadata=None)
    kwargs = {} if native else {"client": "extra binding"}
    async with asyncio.timeout(5):
        pending = asyncio.create_task(wrapper(payload=decoded, **kwargs))
        await entered.wait()
        assert not pending.done()
        release.set()
        result = await pending
    encoded = ActivityTriggerConverter.encode(result, expected_type=None)
    assert json.loads(store._blobs[json.loads(encoded.value)]) == value
    store.download_async.assert_awaited_once()
    store.upload_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_activity_storage_stays_on_invocation_thread(monkeypatch, payload_store_factory):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    value = {"large": "x" * 200}
    token = store.upload(json.dumps(value).encode())
    thread_ids = []
    original_download = store.download
    original_upload = store.upload

    def download(reference):
        thread_ids.append(threading.get_ident())
        return original_download(reference)

    def upload(data, *, instance_id=None):
        thread_ids.append(threading.get_ident())
        return original_upload(data, instance_id=instance_id)

    def activity(payload):
        thread_ids.append(threading.get_ident())
        assert payload == value
        return payload

    monkeypatch.setattr(store, "download", download)
    monkeypatch.setattr(store, "upload", upload)
    wrapper = wrap_activity_payloads(activity, "payload")
    assert not inspect.iscoroutinefunction(wrapper)
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value=token), trigger_metadata=None)
    await asyncio.to_thread(wrapper, decoded)
    assert len(thread_ids) == 3
    assert len(set(thread_ids)) == 1
    assert thread_ids[0] != threading.get_ident()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("operation", ["download", "upload"])
async def test_activity_storage_errors_propagate_without_retry(
        monkeypatch, payload_store_factory, use_async, operation):
    store = payload_store_factory()
    monkeypatch.setattr(payloads, "_payload_store", store)
    token = store.upload(json.dumps("x" * 200).encode())
    error = OSError("payload storage unavailable")
    failure = AsyncMock(side_effect=error) if use_async else Mock(side_effect=error)
    monkeypatch.setattr(store, operation + ("_async" if use_async else ""), failure)
    called = []

    def activity(payload):
        called.append(True)
        return payload

    async def async_activity(payload):
        return activity(payload)

    wrapper = wrap_activity_payloads(async_activity if use_async else activity, "payload")
    decoded = ActivityTriggerConverter.decode(
        meta.Datum(type="string", value=token), trigger_metadata=None)
    with pytest.raises(OSError) as raised:
        if use_async:
            await wrapper(decoded)
        else:
            wrapper(decoded)
    assert raised.value is error
    assert called == ([True] if operation == "upload" else [])
    failure.assert_called_once()
