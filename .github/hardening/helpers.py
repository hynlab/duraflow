import ast
import textwrap
from pathlib import Path


def write(path, content):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(textwrap.dedent(content).lstrip('\n'))


def replace(path, old, new, count=1):
    target = Path(path)
    text = target.read_text()
    if text.count(old) != count:
        raise RuntimeError(f'Patch anchor mismatch: {path}: {old[:90]!r}: {text.count(old)} != {count}')
    target.write_text(text.replace(old, new))


def function(path, qualified, source):
    target = Path(path)
    text = target.read_text()
    node = ast.parse(text)
    for part in qualified.split('.'):
        node = next(child for child in node.body if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and child.name == part)
    start = min([node.lineno] + [d.lineno for d in getattr(node, 'decorator_list', [])]) - 1
    lines = text.splitlines(keepends=True)
    replacement = textwrap.indent(textwrap.dedent(source).strip(), ' ' * node.col_offset) + '\n'
    target.write_text(''.join(lines[:start]) + replacement + ''.join(lines[node.end_lineno:]))


def add_method(path, class_name, source):
    target = Path(path)
    text = target.read_text()
    node = next(child for child in ast.parse(text).body if isinstance(child, ast.ClassDef) and child.name == class_name)
    lines = text.splitlines(keepends=True)
    addition = '\n' + textwrap.indent(textwrap.dedent(source).strip(), '    ') + '\n'
    target.write_text(''.join(lines[:node.end_lineno]) + addition + ''.join(lines[node.end_lineno:]))
