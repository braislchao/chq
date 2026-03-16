#!/usr/bin/env python3
"""Local runner — equivalent to `chq` CLI but runnable without installing the package."""

import sys
from pathlib import Path

# Add src/ to path so we can import chq without installing
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from chq.cli import main

if __name__ == "__main__":
    main()
