"""Check the public splitter against the probe's complete frame oracle."""

import json
from pathlib import Path

import pytest

from bms_flux.frames import Frame, FrameSplitter


@pytest.mark.parametrize("chunk_size", [1, 7, 4096])
def test_every_probe_frame_and_nothing_else(chunk_size: int) -> None:
    raw = b"".join(
        bytes.fromhex(json.loads(line)["hex"])
        for line in Path("probe/fixtures/sniff-19200.raw.jsonl").read_text().splitlines()
    )
    oracle = [
        json.loads(line)
        for line in Path("probe/fixtures/sniff-19200.frames.jsonl").read_text().splitlines()
    ]
    splitter = FrameSplitter()
    frames: list[Frame] = []
    for offset in range(0, len(raw), chunk_size):
        frames.extend(splitter.feed(raw[offset : offset + chunk_size]))
    frames.extend(splitter.feed(b"", final=True))
    assert [(frame.kind, frame.data) for frame in frames] == [
        (record["kind"], bytes.fromhex(record["hex"])) for record in oracle
    ]
    assert len(frames) == 1346
