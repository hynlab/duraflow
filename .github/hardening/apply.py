"""Temporary, allowlisted stage runner used only on the hardening branch."""
import json
import runpy
from pathlib import Path

request = json.loads(Path('.github/hardening/request.json').read_text())
stage = request['stage']
if stage not in {'baseline', 'phase1', 'phase2', 'phase3', 'phase4', 'phase5', 'phase6'}:
    raise SystemExit('Unknown reviewed stage')
runpy.run_path(str(Path('.github/hardening') / (stage + '.py')), run_name='__main__')
