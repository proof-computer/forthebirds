#!/usr/bin/env python3
"""Check the reviewable cache patch accounts for every upstream source change."""
import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
vendor = root / "vendor/rouchdb-adapter-redb"
with tempfile.TemporaryDirectory(prefix="planetarium-vendor-") as directory:
    working = Path(directory)
    shutil.copytree(vendor / "src", working / "src")
    subprocess.run(
        ["git", "apply", "--reverse", "--whitespace=nowarn"],
        input=(vendor / "cache-options.patch").read_bytes(), cwd=working, check=True,
    )
    for line in (vendor / "upstream-sha256.txt").read_text().splitlines():
        expected, name = line.split("  ", 1)
        actual = hashlib.sha256((working / name).read_bytes()).hexdigest()
        if actual != expected:
            raise SystemExit(f"Unrecorded vendor change: {name}")
print("Vendor source matches the pinned upstream plus cache-options.patch")
