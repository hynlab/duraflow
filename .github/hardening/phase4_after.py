from pathlib import Path

path = Path('src/duraflow/client.py')
text = path.read_text()
statement = 'from .contracts import clock_now\n'
if text.count(statement) != 2:
    raise RuntimeError('Unexpected authoritative-clock imports')
path.write_text(text.replace(statement, '', 1))
