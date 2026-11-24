"""Console wiring uses retained bridge availability and a retained offline LWT."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from types import TracebackType
from typing import Any, cast

import aiomqtt
import pytest
from aiohttp import web
from pydantic import SecretStr

from bms_flux.cli import main, run
from bms_flux.settings import MQTTSettings, Pack, Settings
from tests.helpers import FakeTransport, MemoryMQTT


@pytest.mark.asyncio
async def test_run_sets_retained_offline_lwt_and_publishes_online(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings(
        bus="/dev/test",
        packs=[Pack(address=1, serial="PACK")],
        mqtt=MQTTSettings(host="localhost"),
        state_dir=tmp_path,
    )
    messages: list[tuple[str, str, bool]] = []
    online = asyncio.Event()

    class Client:
        async def subscribe(self, topic_filter: str) -> None:
            pass

        async def unsubscribe(self, topic_filter: str) -> None:
            pass

        @property
        def messages(self) -> AsyncIterator[aiomqtt.Message]:
            async def incoming() -> AsyncIterator[aiomqtt.Message]:
                queue: asyncio.Queue[aiomqtt.Message] = asyncio.Queue()
                while True:
                    yield await queue.get()

            return incoming()

        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            pass

        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            messages.append((topic, payload, retain))
            if topic == "bms_flux/status" and payload == "online":
                online.set()

    class Transport(FakeTransport):
        def close(self) -> None:
            pass

    def client(**kwargs: object) -> Client:
        will = kwargs["will"]
        assert isinstance(will, aiomqtt.Will)
        assert will.topic == "bms_flux/status"
        assert will.payload == "offline"
        assert will.retain is True
        return Client()

    async def open_transport(bus: str) -> Transport:
        assert bus == "/dev/test"
        transport = Transport()
        return transport

    monkeypatch.setattr("bms_flux.cli.load_settings", lambda: settings)
    monkeypatch.setattr("bms_flux.cli.aiomqtt.Client", client)
    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", open_transport)
    task = asyncio.create_task(run())
    try:
        async with asyncio.timeout(3):
            await online.wait()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert ("bms_flux/status", "online", True) in messages
    assert ("bms_flux/PACK/availability", "offline", True) in messages


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["", "test-only-token-long"])
async def test_run_only_starts_api_with_token_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, token: str
) -> None:
    settings = Settings(
        bus="/dev/test",
        api_token=SecretStr(token),
        api_port=8877,
        state_dir=tmp_path,
        mqtt=MQTTSettings(host="localhost"),
    )
    events: list[str] = []
    sent: list[tuple[str, Any, bool]] = []
    online = asyncio.Event()
    original_cleanup = web.AppRunner.cleanup

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.sink = MemoryMQTT()
            self.sink.messages = sent

        async def subscribe(self, topic_filter: str) -> None:
            pass

        async def unsubscribe(self, topic_filter: str) -> None:
            pass

        @property
        def messages(self) -> AsyncIterator[aiomqtt.Message]:
            async def incoming() -> AsyncIterator[aiomqtt.Message]:
                await asyncio.Event().wait()
                yield cast(aiomqtt.Message, None)

            return incoming()

        async def publish(
            self, topic: str, payload: str, *, retain: bool = False, qos: int = 0
        ) -> None:
            await self.sink.publish(topic, payload, retain=retain, qos=qos)
            if topic == "bms_flux/status" and payload == "online":
                online.set()

        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            pass

    class Transport(FakeTransport):
        def close(self) -> None:
            events.append("transport closed")

    async def open_transport(bus: str) -> Transport:
        return Transport()

    class Site:
        def __init__(self, runner: web.AppRunner, host: str, port: int) -> None:
            assert host == "0.0.0.0"
            assert port == 8877

        async def start(self) -> None:
            events.append("api started")

    async def cleanup(runner: web.AppRunner) -> None:
        events.append("api cleaned up")
        await original_cleanup(runner)

    monkeypatch.setattr("bms_flux.cli.load_settings", lambda: settings)
    monkeypatch.setattr("bms_flux.cli.aiomqtt.Client", Client)
    monkeypatch.setattr("bms_flux.transport.SerialTransport.open", open_transport)
    monkeypatch.setattr(web, "TCPSite", Site)
    monkeypatch.setattr(web.AppRunner, "cleanup", cleanup)
    # The supervised transport never ends on its own, so stop run() once it's online.
    task = asyncio.create_task(run())
    async with asyncio.timeout(3):
        await online.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert events == (
        ["api started", "api cleaned up", "transport closed"] if token else ["transport closed"]
    )


def test_probe_command_defaults_to_ten_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, int]] = []

    async def probe(bus: str, seconds: int) -> None:
        calls.append((bus, seconds))

    monkeypatch.setattr("bms_flux.cli.probe", probe)
    monkeypatch.setattr("sys.argv", ["bms-flux", "probe", "--bus", "/dev/test"])
    main()
    assert calls == [("/dev/test", 10)]
