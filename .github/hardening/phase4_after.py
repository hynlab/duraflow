from pathlib import Path
from helpers import replace

path = Path('src/duraflow/client.py')
text = path.read_text()
statement = 'from .contracts import clock_now\n'
if text.count(statement) != 2:
    raise RuntimeError('Unexpected authoritative-clock imports')
path.write_text(text.replace(statement, '', 1))
replace('src/duraflow/config.py', '''            value = secret_value(env, key)
            if value is not None:
                source[key] = value''', '''            loaded_secret = secret_value(env, key)
            if loaded_secret is not None:
                source[key] = loaded_secret''')
replace('src/duraflow/retention.py', 'raise Conflict("Retention does not cover the requested safety horizon")', 'raise ValueError("Retention does not cover the requested safety horizon")')
