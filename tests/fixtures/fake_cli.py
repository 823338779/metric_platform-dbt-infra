from __future__ import annotations

import logging
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

SUCCESS_MODE = "success"
FAILURE_MODE = "failure"
SLEEP_MODE = "sleep"
LARGE_OUTPUT_MODE = "large-output"
ECHO_MODE = "echo"
SPLIT_ECHO_MODE = "split-echo"
CHILD_HOLDS_PIPE_MODE = "child-holds-pipe"
STDIN_LENGTH_MODE = "stdin-length"
STDOUT_BEFORE_STDIN_MODE = "stdout-before-stdin"
EXIT_BEFORE_STDIN_MODE = "exit-before-stdin"
NEVER_READ_STDIN_MODE = "never-read-stdin"
ARTIFACT_SUCCESS_MODE = "artifact-success"
ARTIFACT_FAILURE_MODE = "artifact-failure"
SUCCESS_STDOUT = "success stdout"
SUCCESS_STDERR = "success stderr"
FAILURE_STDERR = "failure stderr"
FAILURE_EXIT_CODE = 7


def main() -> int:
    """Expose deterministic subprocess behavior for JobRunner integration tests."""
    mode = sys.argv[1]
    if mode == SUCCESS_MODE:
        print(SUCCESS_STDOUT, flush=True)
        print(SUCCESS_STDERR, file=sys.stderr, flush=True)
        return 0
    if mode == FAILURE_MODE:
        print(FAILURE_STDERR, file=sys.stderr, flush=True)
        return FAILURE_EXIT_CODE
    if mode == SLEEP_MODE:
        time.sleep(float(sys.argv[2]))
        print(SUCCESS_STDOUT, flush=True)
        return 0
    if mode == LARGE_OUTPUT_MODE:
        print(f"prefix-{'x' * int(sys.argv[2])}-tail", flush=True)
        return 0
    if mode == ECHO_MODE:
        print(sys.argv[2], flush=True)
        print(sys.argv[2], file=sys.stderr, flush=True)
        return 0
    if mode == SPLIT_ECHO_MODE:
        split_at = int(sys.argv[3])
        sys.stdout.write(sys.argv[2][:split_at])
        sys.stdout.flush()
        time.sleep(0.05)
        sys.stdout.write(sys.argv[2][split_at:])
        sys.stdout.flush()
        return 0
    if mode == CHILD_HOLDS_PIPE_MODE:
        subprocess.Popen(
            [sys.executable, "-c", f"import time; time.sleep({float(sys.argv[2])})"]
        )
        time.sleep(5)
        return 0
    if mode == STDIN_LENGTH_MODE:
        print(len(sys.stdin.buffer.read()), flush=True)
        return 0
    if mode == STDOUT_BEFORE_STDIN_MODE:
        sys.stdout.write("x" * (256 * 1024))
        sys.stdout.flush()
        print(len(sys.stdin.buffer.read()), flush=True)
        return 0
    if mode == EXIT_BEFORE_STDIN_MODE:
        return FAILURE_EXIT_CODE
    if mode == NEVER_READ_STDIN_MODE:
        time.sleep(5)
        return 0
    if mode in {ARTIFACT_SUCCESS_MODE, ARTIFACT_FAILURE_MODE}:
        import os
        from pathlib import Path

        directory = Path(os.environ["JOB_ARTIFACT_DIR"])
        (directory / "derived.json").write_text("{}", encoding="utf-8")
        print(f"JOB_ARTIFACT_DIR={directory}", flush=True)
        return 0 if mode == ARTIFACT_SUCCESS_MODE else FAILURE_EXIT_CODE
    raise ValueError(f"unsupported fake CLI mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
