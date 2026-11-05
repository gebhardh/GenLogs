"""
vl_logging.py - logging.Handler, der Python-Logs gebündelt an VictoriaLogs sendet.

Nur Standardbibliothek. Sendet im Hintergrund an /insert/jsonline, blockiert
die Anwendung also nicht, und puffert kurzzeitig, wenn VictoriaLogs nicht
erreichbar ist.

Beispiel:
    import logging
    from vl_logging import VictoriaLogsHandler

    handler = VictoriaLogsHandler("http://localhost:9428",
                                  stream_fields=("app", "host"),
                                  extra_fields={"app": "meine-app"})
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

    logging.info("Bestellung angelegt", extra={"order_id": 4711, "kunde": "K-17"})
"""

from __future__ import annotations

import atexit
import json
import logging
import socket
import threading
import time
import traceback
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# Standard-Attribute eines LogRecord; alles andere stammt aus extra={...}
_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


class VictoriaLogsHandler(logging.Handler):
    def __init__(self, url: str = "http://localhost:9428", *,
                 stream_fields: tuple[str, ...] = ("app", "host"),
                 extra_fields: dict | None = None,
                 flush_interval: float = 1.0,
                 batch_size: int = 500,
                 max_buffer: int = 50_000,
                 timeout: float = 5.0,
                 headers: dict | None = None,
                 level: int = logging.NOTSET):
        super().__init__(level)
        params = urllib.parse.urlencode({"_stream_fields": ",".join(stream_fields)})
        self.endpoint = url.rstrip("/") + "/insert/jsonline?" + params
        # Ohne diesen Content-Type verwirft VictoriaLogs den Body stillschweigend!
        self.headers = {"Content-Type": "application/stream+json", **(headers or {})}
        self.static = {"host": socket.gethostname(), **(extra_fields or {})}
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self.max_buffer = max_buffer
        self.timeout = timeout

        self._buf: list[str] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="vl-log-sender", daemon=True)
        self._thread.start()
        atexit.register(self.close)

    # -- Aufbereitung --------------------------------------------------------
    def _to_dict(self, record: logging.LogRecord) -> dict:
        d = {
            "_time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "_msg": record.getMessage(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "pid": record.process,
            "thread": record.threadName,
            **self.static,
        }
        if record.exc_info:
            d["exception"] = "".join(traceback.format_exception(*record.exc_info))
        for k, v in vars(record).items():           # Felder aus extra={...}
            if k not in _RESERVED and not k.startswith("_"):
                d[k] = v
        return d

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = json.dumps(self._to_dict(record), ensure_ascii=False, default=str)
        except Exception:
            self.handleError(record)
            return
        with self._lock:
            self._buf.append(line)
            if len(self._buf) > self.max_buffer:          # älteste verwerfen
                del self._buf[: len(self._buf) - self.max_buffer]
            if len(self._buf) >= self.batch_size:
                self._wake.set()

    # -- Versand -------------------------------------------------------------
    def _send(self, lines: list[str]) -> bool:
        body = ("\n".join(lines) + "\n").encode("utf-8")
        req = urllib.request.Request(self.endpoint, data=body, method="POST", headers=self.headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                r.read()
            return True
        except OSError:
            return False

    def flush(self) -> None:
        with self._lock:
            lines, self._buf = self._buf, []
        if lines and not self._send(lines):
            with self._lock:                             # zurück in den Puffer
                self._buf[:0] = lines

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.flush_interval)
            self._wake.clear()
            self.flush()

    def close(self) -> None:
        if not self._stop.is_set():
            self._stop.set()
            self._wake.set()
            self._thread.join(timeout=self.timeout + 1)
            self.flush()
        super().close()


if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9428"
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().addHandler(VictoriaLogsHandler(url, extra_fields={"app": "vl-logging-test"}))
    log = logging.getLogger("demo")
    log.info("Hallo VictoriaLogs", extra={"order_id": 4711})
    try:
        1 / 0
    except ZeroDivisionError:
        log.exception("Rechenfehler")
    time.sleep(0.2)
