#!/usr/bin/env python3
"""Convert a completed native YDB dump using DescribeTable metadata."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import unquote_to_bytes
from native_io import configure_csv


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def type_info(column):
    value = column['type']
    nullable = 'optional_type' in value
    if nullable:
        value = value['optional_type']['item']
    return value['type_id'], nullable


def decode_cell(raw, column):
    kind, nullable = type_info(column)
    if raw == 'null':
        if not nullable:
            raise ValueError('NULL in non-nullable field: ' + column['name'])
        return None
    if kind in ('UTF8', 'JSON', 'JSON_DOCUMENT'):
        if not (raw.startswith('"') and raw.endswith('"')):
            raise ValueError('Expected URL-encoded quoted string: ' + column['name'])
        # YDB uses form-style URL encoding: '+' means a space, '%2B' a plus.
        return unquote_to_bytes(raw[1:-1].replace('+', ' ')).decode('utf-8', errors='strict')
    if kind == 'UINT32':
        value = int(raw)
        if not 0 <= value <= 4294967295:
            raise ValueError('UINT32 outside source range')
        return value
    if kind in ('INT32', 'INT64'):
        value = int(raw)
        bits = 32 if kind == 'INT32' else 64
        if not -(2**(bits-1)) <= value < 2**(bits-1):
            raise ValueError('Signed integer outside source range')
        return value
    if kind == 'BOOL':
        if raw not in ('true', 'false'):
            raise ValueError('Unexpected BOOL representation')
        return raw == 'true'
    if kind in ('DATETIME', 'TIMESTAMP'):
        value = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if value.tzinfo is None:
            raise ValueError('Missing timezone')
        return value.astimezone(timezone.utc).isoformat()
    raise ValueError('Unsupported YDB type: ' + kind)


def main():
    os.umask(0o077)
    configure_csv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path, help='Download directory containing native-dump, objects.json and <table>.schema.json')
    parser.add_argument('--database', required=True, help='Full source YDB database path')
    args = parser.parse_args()
    root = args.directory.resolve()
    native = root / 'native-dump'
    normalized = root / 'jsonl'
    if normalized.exists() or (root / 'postgres').exists():
        raise ValueError('Conversion output exists; choose a new directory')
    objects = json.loads((root / 'objects.json').read_text())
    table_names = sorted(item['path'] for item in objects if item['type'] == 'table' and not item['path'].startswith('.sys/'))
    if not table_names:
        raise ValueError('No tables found')
    if list(native.rglob('incomplete*')):
        raise ValueError('Native dump is incomplete')
    schema = ['BEGIN;', 'SET LOCAL search_path = public;']
    indexes, checks, descriptions, results = [], [], [], []
    mapping = {'UINT32': 'BIGINT', 'UTF8': 'TEXT', 'JSON': 'JSONB', 'DATETIME': 'TIMESTAMPTZ', 'TIMESTAMP': 'TIMESTAMPTZ', 'BOOL': 'BOOLEAN', 'INT32': 'BIGINT', 'INT64': 'BIGINT', 'JSON_DOCUMENT': 'JSONB'}
    normalized.mkdir()
    for name in table_names:
        if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', name) or len(name.encode()) > 63:
            raise ValueError('Unexpected table path; explicit mapping required')
        folder = native / name
        if (folder / 'incomplete').exists():
            raise ValueError('Incomplete table: ' + name)
        description = json.loads((root / (name + '.schema.json')).read_text())
        if description.get('ttl_settings') or description.get('changefeeds'):
            raise ValueError('TTL/CDC requires explicit migration design: ' + name)
        columns = description['columns']
        for column in columns:
            if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', column['name']) or len(column['name'].encode()) > 63:
                raise ValueError('Column name needs explicit mapping')
            if type_info(column)[0] not in mapping:
                raise ValueError('Unsupported YDB type: ' + type_info(column)[0])
        names = [column['name'] for column in columns]
        dump_columns = re.findall(r'columns\s*\{\s*name:\s*"([^"]+)"', (folder / 'scheme.pb').read_text())
        if names != dump_columns:
            raise ValueError('Column order differs between dump and DescribeTable: ' + name)
        definitions = []
        for column in columns:
            kind, nullable = type_info(column)
            definitions.append('    ' + quote(column['name']) + ' ' + mapping[kind] + ('' if nullable and column['name'] not in description['primary_key'] else ' NOT NULL'))
            if kind == 'UINT32':
                checks.append('ALTER TABLE public.' + quote(name) + ' ADD CHECK (' + quote(column['name']) + ' BETWEEN 0 AND 4294967295);')
        definitions.append('    PRIMARY KEY (' + ', '.join(quote(key) for key in description['primary_key']) + ')')
        schema.append('CREATE TABLE public.' + quote(name) + ' (\n' + ',\n'.join(definitions) + '\n);')
        for index in description.get('indexes', []):
            if 'global_index' not in index or any(key in index for key in ('global_async_index', 'global_vector_kmeans_tree_index', 'global_unique_index')):
                raise ValueError('Special index requires explicit mapping: ' + name)
            if not index.get('index_columns') or not set(index['index_columns']) <= set(names):
                raise ValueError('Invalid index columns')
            if len((name + '_' + index['name']).encode()) > 63:
                raise ValueError('PostgreSQL index name exceeds 63 bytes')
            indexes.append('CREATE INDEX ' + quote(name + '_' + index['name']) + ' ON public.' + quote(name) + ' (' + ', '.join(quote(key) for key in index['index_columns']) + ');')
        descriptions.append({'table': name, 'columns': [{'name': c['name'], 'ydb_type': type_info(c)[0], 'nullable': type_info(c)[1], 'postgres_nullable': type_info(c)[1] and c['name'] not in description['primary_key'], 'postgres_type': mapping[type_info(c)[0]]} for c in columns], 'primary_key': description['primary_key'], 'indexes': description.get('indexes', [])})
        output = normalized / (name + '.jsonl')
        files = sorted(folder.glob('data_*.csv'))
        if not files:
            raise ValueError('No completed data files: ' + name)
        count = 0
        digest = hashlib.sha256()
        with output.open('wb') as target:
            for path in files:
                with path.open(encoding='utf-8', newline='') as stream:
                    # URL-encoding protects delimiters. Retain quotes to distinguish
                    # the optional NULL marker null from literal UTF8 "null".
                    for tokens in csv.reader(stream, quoting=csv.QUOTE_NONE):
                        if len(tokens) != len(columns):
                            raise ValueError('CSV field count mismatch: ' + name)
                        row = {column['name']: decode_cell(raw, column) for raw, column in zip(tokens, columns)}
                        for key in description['primary_key']:
                            if row[key] is None:
                                raise ValueError('NULL in YDB primary key: ' + name + '.' + key + '; PostgreSQL PRIMARY KEY requires non-NULL values. An explicit schema redesign is required; no replacement value was generated.')
                        line = (json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(',', ':')) + '\n').encode()
                        target.write(line)
                        digest.update(line)
                        count += 1
                        if count % 25000 == 0:
                            print(json.dumps({'table': name, 'decoded_rows': count}), flush=True)
        results.append({'table': name, 'rows': count, 'sha256': digest.hexdigest(), 'source_files': [str(f.relative_to(root)) for f in files], 'complete': True})
        print(json.dumps({'table': name, 'rows': count, 'complete': True}), flush=True)
    simple_schema = []
    for statement in schema:
        simple_schema.append(statement)
    simple_schema.extend(indexes)
    simple_schema.extend(checks)
    simple_schema.append('COMMIT;')
    schema_path = root / '001_initial_schema.sql'
    schema_path.write_text('\n\n'.join(simple_schema) + '\n', encoding='utf-8')
    manifest = {'database': args.database, 'native_dump_complete': True, 'common_snapshot_across_tables': False, 'export_mode': 'YDB tools dump --avoid-copy --ordered', 'tables': results}
    (normalized / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    (root / 'schema-mapping.json').write_text(json.dumps(descriptions, ensure_ascii=False, indent=2) + '\n')
    from jsonl_to_postgres import convert
    report = convert(normalized, schema_path, root / 'postgres')
    print(json.dumps({'converted_rows': report['rows'], 'tables': len(report['tables'])}), flush=True)


if __name__ == '__main__':
    main()
