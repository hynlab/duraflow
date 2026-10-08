"""Capture source-only diagnostics, never runtime data, environment or Git credentials."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

with ZipFile('source-snapshot.zip', 'w', ZIP_DEFLATED) as archive:
    for directory in ('src', 'tests', 'scripts', 'docs', 'examples'):
        for path in Path(directory).rglob('*'):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix in {'.py', '.json', '.md', '.sql', '.yaml', '.yml', '.typed'}:
                archive.write(path)
    for name in ('Makefile', 'pyproject.toml', 'README.md', 'LICENSE', 'compose.yaml'):
        if Path(name).is_file():
            archive.write(name)
