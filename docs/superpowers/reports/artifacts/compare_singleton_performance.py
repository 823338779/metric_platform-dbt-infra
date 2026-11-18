"""Compare the singleton benchmark: script CURRENT_NATIVE BASELINE_NATIVE OUTPUT_JSONL.

Runs a fixed ABBA/ABBA sequence without editing helpers or acceptance thresholds.
Use an existing native Hatch Python environment for both revisions.
"""

from __future__ import annotations

import gc
import hashlib
import json
import os
import subprocess
import sys
import time
import timeit
from collections.abc import Callable
from pathlib import Path


def child(root: str) -> None:
    os.chdir(root)
    sys.path.insert(0, root)
    from metricflow_semantics.test_helpers.performance import performance_helpers as helper
    from metricflow_semantics.toolkit import dataclass_helpers, singleton
    from tests_metricflow_semantics.toolkit import singleton_test_classes as classes
    from tests_metricflow_semantics.toolkit import test_singleton as test

    setup = test.setup_statement.__wrapped__()
    params = {}
    test.assert_performance_factor = lambda **kwargs: params.update(kwargs)
    test.test_create_existing(setup)
    timer_type = timeit.Timer
    measurements = []

    class RecordingTimer:
        def __init__(self, *, timer: Callable[[], float], setup: str, stmt: str) -> None:
            self.timer = timer_type(timer=timer, setup=setup, stmt=stmt)

        def autorange(self) -> tuple[int, float]:
            result = self.timer.autorange()
            measurements.append({"iterations": result[0], "seconds": result[1]})
            return result

    timeit.Timer = RecordingTimer
    try:
        helper.assert_performance_factor(**params)
        outcome = "pass"
    except AssertionError:
        outcome = "fail"
    finally:
        timeit.Timer = timer_type

    precise = []
    for index in range(3):
        values = {}
        for side in ("left", "right") if index % 2 == 0 else ("right", "left"):
            gc.collect()
            timer = timer_type(setup=params[side + "_setup"], stmt=params[side + "_statement"], timer=time.perf_counter)
            values[side] = timer.timeit(number=300) / 300
        precise.append(dict(**values, ratio=values["left"] / values["right"]))

    result = {
        "root": root,
        "executable": sys.executable,
        "hashseed": os.environ.get("PYTHONHASHSEED"),
        "outcome": outcome,
        "cpu_measurements": measurements,
        "perf_counter": precise,
        "source_hashes": {
            m.__name__: hashlib.sha256(Path(m.__file__).read_text().encode()).hexdigest()
            for m in (test, classes, helper, singleton, dataclass_helpers)
        },
        "source_paths": {m.__name__: m.__file__ for m in (test, singleton)},
        "clock_info": {name: vars(time.get_clock_info(name)) for name in ("thread_time", "perf_counter")},
    }
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--child":
        child(sys.argv[2])
    else:
        if len(sys.argv) != 4:
            raise SystemExit("Usage: compare_singleton_performance.py CURRENT_NATIVE BASELINE_NATIVE OUTPUT_JSONL")
        current = Path(sys.argv[1]).resolve()
        baseline = Path(sys.argv[2]).resolve()
        output = Path(sys.argv[3]).resolve()
        env = dict(os.environ, PYTHONHASHSEED="0", PYTHONUTF8="1")
        with output.open("w", encoding="utf-8") as stream:
            for label in ("baseline", "current", "current", "baseline") * 2:
                root = baseline if label == "baseline" else current
                run = subprocess.run(
                    [sys.executable, __file__, "--child", str(root)],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                record = json.loads(run.stdout)
                record["revision"] = label
                stream.write(json.dumps(record) + "\n")
                stream.flush()
                print(label, record["outcome"], flush=True)
