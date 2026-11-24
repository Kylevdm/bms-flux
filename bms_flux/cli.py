"""Run the bms-flux serial-to-MQTT bridge from the console script."""

import argparse
import asyncio
from contextlib import suppress

import aiomqtt
from pydantic import ValidationError

from bms_flux.bridge import Bridge
from bms_flux.clock import SystemClock
from bms_flux.mqtt import BrokerSink
from bms_flux.protocol.seplos_v3 import SeplosV3Decoder
from bms_flux.settings import load_settings
from bms_flux.transport import SerialTransport


class ConfigurationError(Exception):
    """A configuration failure safe to report without including secret input."""


async def run() -> None:
    try:
        settings = await asyncio.to_thread(load_settings)
    except ValidationError as error:
        problems = "\n".join(
            f"  {'.'.join(map(str, problem['loc']))}: {problem['msg']}"
            for problem in error.errors(include_input=False, include_url=False)
        )
        raise ConfigurationError(f"invalid configuration:\n{problems}") from None
    mqtt = settings.mqtt
    async with aiomqtt.Client(
        hostname=mqtt.host,
        port=mqtt.port,
        username=mqtt.username,
        password=mqtt.password.get_secret_value() if mqtt.password else None,
    ) as client:
        transport = await SerialTransport.open(settings.bus)
        try:
            await Bridge(
                transport,
                SeplosV3Decoder(settings.packs),
                BrokerSink(client),
                settings.packs,
                SystemClock(),
                snapshot_interval_s=settings.snapshot_interval_s,
            ).run()
        finally:
            transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="bms-flux")
    parser.add_subparsers(dest="command", required=True).add_parser("run")
    parser.parse_args()
    with suppress(KeyboardInterrupt):
        try:
            asyncio.run(run())
        except ConfigurationError as error:
            parser.exit(2, f"bms-flux: {error}\n")
