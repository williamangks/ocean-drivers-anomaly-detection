#!/usr/bin/env python3
from __future__ import annotations

import os

def env_default(name: str, default: str) -> str:
    return os.getenv(name, default)

def env_required(name: str) -> bool:
    # Required only if not set in environment
    return os.getenv(name) is None
