"""Run the bms-flux serial-to-MQTT bridge from the console script."""

import argparse
import asyncio
from contextlib import suppress

import aiomqtt
from pydantic import ValidationError

from bms_flux.bridge import Bridge, probe_blocks
from bms_flux.clock import Clock, SystemClock
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
        will=aiomqtt.Will("bms_flux/status", "offline", retain=True),
    ) as client:
        transport = await SerialTransport.open(settings.bus)
        try:
            await Bridge(
                transport,
                SeplosV3Decoder(),
                BrokerSink(client),
                settings.packs,
                SystemClock(),
                snapshot_interval_s=settings.snapshot_interval_s,
                silent_after_s=settings.silent_after_s,
            ).run()
        finally:
            transport.close()


async def probe(bus: str, seconds: int = 10, *, clock: Clock | None = None) -> None:
    transport = await SerialTransport.open(bus)
    try:
        heard = await probe_blocks(transport, SeplosV3Decoder(), clock or SystemClock(), seconds)
        for address, block in sorted(heard.items()):
            cycles = block.fields.get("cycles", "unknown")
            soc = block.fields.get("reported_soc")
            reported_soc = "unknown" if soc is None else f"{soc}%"
            print(f"bus address {address}: cycles={cycles}, reported SoC={reported_soc}")
    finally:
        transport.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="bms-flux")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run")
    probe_parser = commands.add_parser(
        "probe", help="listen for bus addresses without transmitting"
    )
    probe_parser.add_argument("--bus", required=True)
    probe_parser.add_argument("--seconds", type=int, default=10)
    args = parser.parse_args()
    if args.command == "probe" and args.seconds <= 0:
        parser.error("--seconds must be positive")
    with suppress(KeyboardInterrupt):
        try:
            asyncio.run(probe(args.bus, args.seconds) if args.command == "probe" else run())
        except ConfigurationError as error:
            parser.exit(2, f"bms-flux: {error}\n")
