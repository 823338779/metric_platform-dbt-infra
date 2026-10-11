"""引擎输出的增量脱敏和有界尾部缓冲。"""

import codecs
from collections.abc import Iterable

SECRET_ENVIRONMENT_PREFIX = "DBT_ENV_SECRET_"
REDACTION_MARKER = "***"


class _TailBuffer:
    """Continuously retain only the bounded byte tail of one output stream."""

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._data = bytearray()
        self.truncated = False

    def append(self, chunk: bytes) -> None:
        """Add a redacted stream chunk and discard bytes outside the tail."""
        self._data.extend(chunk)
        if len(self._data) > self._max_bytes:
            del self._data[: len(self._data) - self._max_bytes]
            self.truncated = True

    def decode(self) -> str:
        """Decode retained bytes even when truncation split a code point."""
        return self._data.decode("utf-8", errors="replace")


class _RedactingTailBuffer:
    """Redact exact secrets across chunks before applying the byte tail limit."""

    def __init__(self, max_bytes: int, secrets: Iterable[str]) -> None:
        self._tail = _TailBuffer(max_bytes)
        self._secrets = tuple(sorted(set(secrets), key=len, reverse=True))
        self._max_secret_length = max((len(secret) for secret in self._secrets), default=0)
        self._pending = ""
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    @property
    def truncated(self) -> bool:
        """Return whether redacted output exceeded the configured byte limit."""
        return self._tail.truncated

    def feed(self, chunk: bytes) -> None:
        """Decode and redact complete matches while retaining a cross-chunk suffix."""
        self._process(self._decoder.decode(chunk, final=False), final=False)

    def finish(self) -> None:
        """Flush the decoder and redact any remaining suffix at stream EOF."""
        self._process(self._decoder.decode(b"", final=True), final=True)

    def decode(self) -> str:
        """Return the retained redacted text tail."""
        return self._tail.decode()

    def _process(self, text: str, *, final: bool) -> None:
        combined = self._pending + text
        if final or self._max_secret_length == 0:
            cutoff = len(combined)
        else:
            cutoff = max(0, len(combined) - self._max_secret_length + 1)

        output: list[str] = []
        index = 0
        while index < cutoff:
            secret = next(
                (candidate for candidate in self._secrets if combined.startswith(candidate, index)),
                None,
            )
            if secret is not None:
                output.append(REDACTION_MARKER)
                index += len(secret)
            else:
                output.append(combined[index])
                index += 1
        self._pending = combined[index:]
        if output:
            self._tail.append("".join(output).encode("utf-8"))
