import os
import select
import signal
import subprocess
import sys

import pytest


def test_stuck_synchronous_worker_process_obeys_hard_shutdown_boundary():
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.supervision_process"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ},
    )
    try:
        if not select.select([process.stdout], [], [], 10)[0]:
            process.kill()
            _, stderr = process.communicate(timeout=5)
            pytest.fail("Shutdown fixture did not start: " + stderr[-2000:])
        assert process.stdout.readline().strip() == "READY"
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=5)
        assert process.returncode == 75
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
