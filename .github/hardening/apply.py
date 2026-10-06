"""Apply one reviewed stage exactly once; never replay a committed migration recipe."""
import json
import runpy
from pathlib import Path

stage = json.loads(Path('.github/hardening/request.json').read_text())['stage']
if stage not in {'phase5', 'phase6'}:
    raise SystemExit('Unknown reviewed qualification stage')
if not Path(f'docs/hardening/{stage}.md').exists():
    runpy.run_path(f'.github/hardening/{stage}.py', run_name='__main__')
    after = Path(f'.github/hardening/{stage}_after.py')
    if after.exists():
        runpy.run_path(str(after), run_name='__main__')
