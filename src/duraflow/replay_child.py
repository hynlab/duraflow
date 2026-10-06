"""Private JSON replay worker. Only a configured trusted app can register code."""

from __future__ import annotations

import argparse
import importlib
import os
import sys

from .contracts import Registry, canonical, parse_json
from .replay import replay


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    args = parser.parse_args()
    protocol_output = sys.stdout.buffer
    # Application prints and exception traces are not the protocol or diagnostics.
    with open(os.devnull, "w") as discarded:
        sys.stdout = discarded
        sys.stderr = discarded
        try:
            app = importlib.import_module(args.app)
            registry = app.registry
            if not isinstance(registry, Registry):
                return
        except Exception:
            return
        protocol_output.write(b'{"ready":true}\n')
        protocol_output.flush()
        while True:
            raw = sys.stdin.buffer.readline(args.max_bytes + 1)
            if not raw:
                return
            if len(raw) > args.max_bytes or not raw.endswith(b"\n"):
                return
            try:
                state = parse_json(raw)["state"]
                definition = registry.match_manifest(state["manifest"])
                if definition is None:
                    response = {"ok": False, "code": "MissingImplementation"}
                else:
                    activation = replay(definition, state)
                    response = {"ok": True, "kind": activation.kind, "value": activation.value}
            except Exception as exc:
                response = {"ok": False, "code": type(exc).__name__}
            encoded = canonical(response).encode() + b"\n"
            if len(encoded) > args.max_bytes:
                return
            protocol_output.write(encoded)
            protocol_output.flush()


if __name__ == "__main__":
    main()
