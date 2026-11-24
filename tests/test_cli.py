"""Console wiring uses retained bridge availability and a retained offline LWT."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from types import TracebackType

import aiomqtt
import pytest

from bms_flux.cli import main, run
from bms_flux.settings import MQTTSettings, Pack, Settings
from tests.helpers import FakeTransport


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


def test_probe_command_defaults_to_ten_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, int]] = []

    async def probe(bus: str, seconds: int) -> None:
        calls.append((bus, seconds))

    monkeypatch.setattr("bms_flux.cli.probe", probe)
    monkeypatch.setattr("sys.argv", ["bms-flux", "probe", "--bus", "/dev/test"])
    main()
    assert calls == [("/dev/test", 10)]
