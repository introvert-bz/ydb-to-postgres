"""Shared native-dump validation; no cloud access and no cell values in errors."""
import csv
import hashlib
import json
from pathlib import Path
import re
import sys


def configure_csv():
    # URL encoding can expand one source byte to three CSV characters.
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def count_rows(path, column_count):
    configure_csv()
    count = 0
    with path.open(encoding='utf-8', newline='') as stream:
        for fields in csv.reader(stream, quoting=csv.QUOTE_NONE):
            if len(fields) != column_count:
                raise ValueError('Native CSV column count mismatch: ' + path.parent.name)
            count += 1
    return count


def file_record(root, path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return {'path': path.relative_to(root).as_posix(), 'bytes': path.stat().st_size, 'sha256': digest.hexdigest()}


def native_records(root):
    native = root / 'native-dump'
    if not native.is_dir() or native.is_symlink():
        raise ValueError('Missing native-dump directory or unexpected symlink')
    records = []
    for path in sorted(native.rglob('*')):
        if path.is_symlink():
            raise ValueError('Native dump contains a symlink')
        if path.name.startswith('incomplete'):
            raise ValueError('Native dump contains an incomplete marker')
        if path.is_file() and '.sys' not in path.relative_to(root).parts:
            records.append(file_record(root, path))
    return records


def validate_native(root, manifest):
    tables = manifest['tables']
    names = [table['table'] for table in tables]
    if not names or len(names) != len(set(names)) or any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', name) for name in names):
        raise ValueError('Invalid or duplicate native table names')
    objects = json.loads((root / 'objects.json').read_text(encoding='utf-8'))
    discovered = {item['path'] for item in objects if item['type'] == 'table' and not item['path'].startswith('.sys/')}
    if set(names) != discovered:
        raise ValueError('Download manifest does not match the discovered table list')
    records = native_records(root)
    for table in tables:
        name = table['table']
        schema_path = root / (name + '.schema.json')
        if schema_path.is_symlink():
            raise ValueError('Unexpected schema symlink: ' + name)
        schema = json.loads(schema_path.read_text(encoding='utf-8'))
        columns = [column['name'] for column in schema['columns']]
        scheme = root / 'native-dump' / name / 'scheme.pb'
        if not scheme.is_file() or not columns:
            raise ValueError('Missing native table schema: ' + name)
        dump_columns = re.findall(r'columns\s*\{\s*name:\s*"([^"]+)"', scheme.read_text(encoding='utf-8'))
        if dump_columns != columns:
            raise ValueError('Native schema column order mismatch: ' + name)
        files = sorted(scheme.parent.glob('data_*.csv'))
        if not files or not all(path.is_file() for path in files):
            raise ValueError('Missing native table data: ' + name)
        actual = sum(count_rows(path, len(columns)) for path in files)
        if type(table['rows']) is not int or table['rows'] < 0 or actual != table['rows']:
            raise ValueError('Native row count differs from download manifest: ' + name)
    if 'native_files' in manifest and records != manifest['native_files']:
        raise ValueError('Native files differ from download checksums; retain the files for diagnosis')
    return records
