"""Temporary, allowlisted stage runner used only on the hardening branch."""
import json
import runpy
from pathlib import Path

request = json.loads(Path('.github/hardening/request.json').read_text())
stage = request['stage']
if stage not in {'baseline', 'phase1', 'phase2', 'phase3', 'phase4', 'phase5', 'phase6'}:
    raise SystemExit('Unknown reviewed stage')
runpy.run_path(str(Path('.github/hardening') / (stage + '.py')), run_name='__main__')
if stage == 'phase1':
    runpy.run_path('.github/hardening/phase1_after.py', run_name='__main__')
    matrix = Path('scripts/codec_matrix.py')
    matrix.write_text(matrix.read_text().replace('import sys\n', ''))
if stage == 'phase2':
    tests = Path('tests/test_phase2.py')
    tests.write_text(tests.read_text().replace('mutate, new_run, route', 'mutate, route'))
