"""Temporary stage runner; never shipped to main or a distribution."""
import json
import runpy
from pathlib import Path
from helpers import function

stage = json.loads(Path('.github/hardening/request.json').read_text())['stage']
if stage not in {'baseline', 'phase1', 'phase2', 'phase3', 'phase4', 'phase5', 'phase6'}:
    raise SystemExit('Unknown reviewed stage')
if stage != 'phase3' or not Path('docs/hardening/phase3.md').is_file():
    runpy.run_path(str(Path('.github/hardening') / (stage + '.py')), run_name='__main__')
    extra = Path('.github/hardening') / (stage + '_after.py')
    if extra.is_file():
        runpy.run_path(str(extra), run_name='__main__')
if stage == 'phase3':
    function('src/duraflow/observability.py', 'JsonLogFormatter.format', '''
    def format(self, record: logging.LogRecord) -> str:
        event = str(record.msg)
        if event not in EVENTS:
            event = "runtime_message"
        data: dict[str, Any] = {"time": record.created, "level": record.levelname, "event": event}
        for key in SAFE_FIELDS:
            value: object = getattr(record, key, None)
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", value):
                data[key] = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    if math.isfinite(float(value)):
                        data[key] = value
                except OverflowError:
                    pass
        if record.exc_info and record.exc_info[0]:
            data["error_type"] = record.exc_info[0].__name__
        return json.dumps(data, separators=(",", ":"), allow_nan=False)
    ''')
