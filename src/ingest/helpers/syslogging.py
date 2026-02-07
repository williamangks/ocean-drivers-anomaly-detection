#!/usr/bin/env python3

from typing import Literal, Protocol

LogLevel = Literal['ERROR', 'INFO', 'DEBUG']

class LogFn(Protocol):
    def __call__(self, msg: str, level: LogLevel = 'INFO') -> None: ...

def make_logger(
    min_level: str,
    driver: str
) -> LogFn:
    levels: dict[str, int] = {'ERROR': 0, 'INFO': 1, 'DEBUG': 2}
    min_level = min_level.upper()
    if min_level not in levels:
        min_level = 'INFO'

    def _log(msg: str, driver=driver, level: LogLevel = 'INFO') -> None:
        lvl = level.upper()
        if lvl not in levels:
            lvl = 'INFO'
        if levels[lvl] <= levels[min_level]:
            print(f"[{driver}] {lvl} {msg}", flush=True)

    return _log
