"""Additional first-stage boundary cases; never modifies workflow permissions."""
import ast
from pathlib import Path
from helpers import function, replace, write

function('tests/test_phase1.py', 'claim', '''
async def claim(env, handle):
    await env.engine.tick()
    await env.engine.tick()
    delivery = await env.transport.receive(route(env.namespace, DOUBLE.descriptor()), 'workers')
    assert delivery is not None
    context = await env.worker._claim(delivery, DOUBLE)
    assert context is not None
    return context
''')
path = Path('src/duraflow/runner.py')
text = path.read_text()
root = ast.parse(text)
cls = next(n for n in root.body if isinstance(n, ast.ClassDef) and n.name == 'Worker')
method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_claim')
change = next(n for n in method.body if isinstance(n, ast.FunctionDef) and n.name == 'change')
lines = text.splitlines(keepends=True)
line = change.body[0].lineno - 1
lines.insert(line, '            if state.get("codec_version", 1) != 1:\n                raise ProtocolError("Unsupported durable codec version")\n')
path.write_text(''.join(lines))
# Do not recycle a malformed response before validating its activation discriminator.
replace('src/duraflow/executor.py', '                self._idle.append(process)', '                if response["ok"] and response.get("kind") not in {"schedule", "waiting", "completed", "failed"}:\n                    raise WorkflowBlocked("REPLAY_EXECUTOR_INVALID_RESPONSE")\n                self._idle.append(process)')
write('scripts/codec_matrix.py', '''
"""Run immutable codec fixtures in isolated, explicitly selected dependency environments."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import venv


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--versions', nargs='+', default=['2.13.4', '2.13.5'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    for version in args.versions:
        if not version.replace('.', '').isdigit():
            raise SystemExit('Versions must be explicit numeric release identifiers')
        with tempfile.TemporaryDirectory(prefix='duraflow-codec-') as directory:
            venv.EnvBuilder(with_pip=True).create(directory)
            python = Path(directory) / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
            subprocess.run([str(python), '-m', 'pip', 'install', '--quiet', f'pydantic=={version}',
                            'pytest>=8,<10', 'pytest-asyncio>=0.24,<2'], check=True, timeout=180)
            env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(root / 'src'), str(root)])}
            env.pop('DURAFLOW_REQUIRE_NATIVE', None)
            subprocess.run([str(python), '-m', 'pytest', '-q', 'tests/test_phase1.py', '-k', 'frozen'],
                           cwd=root, env=env, check=True, timeout=60)
            print(f'Frozen payload/history fixtures passed: pydantic {version}', flush=True)


if __name__ == '__main__':
    main()
''')
# This is the explicitly qualified codec range for the next candidate, not the old PyPI artifact.
replace('pyproject.toml', '"pydantic>=2.10,<3"', '"pydantic>=2.13.4,<2.14"')
with Path('Makefile').open('a') as stream:
    stream.write('\n.PHONY: codec-check\ncodec-check:\n\tpython scripts/codec_matrix.py\n')
replace('Makefile', '\tDURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. pytest', '\tpython scripts/codec_matrix.py\n\tDURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. pytest')
with Path('docs/hardening/phase1.md').open('a') as stream:
    stream.write('''\nThe next candidate narrows Pydantic to >=2.13.4,<2.14. The two explicitly
qualified patch versions are exercised by scripts/codec_matrix.py against the
same immutable fixtures. Adopting another minor version requires compatibility
review, not silent recomputation of schema identifiers. Users of alpha with a
different Pydantic minor must retain their old engine/worker environment until
their own saved schema fixtures pass; this does not rewrite alpha data.\n''')
