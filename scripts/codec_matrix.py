"""Run immutable codec fixtures in isolated, explicitly selected dependency environments."""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile
import venv


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--versions", nargs="+", default=["2.13.4", "2.13.5"])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    for version in args.versions:
        if not version.replace(".", "").isdigit():
            raise SystemExit("Versions must be explicit numeric release identifiers")
        with tempfile.TemporaryDirectory(prefix="duraflow-codec-") as directory:
            venv.EnvBuilder(with_pip=True).create(directory)
            python = Path(directory) / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            subprocess.run(
                [
                    str(python),
                    "-m",
                    "pip",
                    "install",
                    "--quiet",
                    f"pydantic=={version}",
                    "pytest>=8,<10",
                    "pytest-asyncio>=0.24,<2",
                ],
                check=True,
                timeout=180,
            )
            env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(root / "src"), str(root)])}
            env.pop("DURAFLOW_REQUIRE_NATIVE", None)
            subprocess.run(
                [str(python), "-m", "pytest", "-q", "tests/test_phase1.py", "-k", "frozen"],
                cwd=root,
                env=env,
                check=True,
                timeout=60,
            )
            print(f"Frozen payload/history fixtures passed: pydantic {version}", flush=True)


if __name__ == "__main__":
    main()
