#!/usr/bin/env python3
"""Convert YDB CLI JSON Lines exports to PostgreSQL COPY scripts (stdlib only)."""
import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import zipfile


def split_definitions(body):
    start = depth = 0
    for index, char in enumerate(body):
        depth += (char == '(') - (char == ')')
        if char == ',' and depth == 0:
            yield body[start:index].strip()
            start = index + 1
    yield body[start:].strip()


def parse_schema(sql):
    """Deliberately supports the simple DDL supplied with this converter."""
    clean = re.sub(r'--[^\n]*', '', sql)
    tables = {}
    pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:public\.)?\"?(\w+)\"?\s*\((.*?)\)\s*;'
    for match in re.finditer(pattern, clean, re.I | re.S):
        name, body = match.groups()
        columns, keys = [], []
        for definition in split_definitions(body):
            primary = re.fullmatch(r'PRIMARY\s+KEY\s*\(([^)]+)\)', definition, re.I)
            if primary:
                keys.extend(key.strip().strip(chr(34)) for key in primary[1].split(','))
                continue
            column = re.fullmatch(r'"?(\w+)"?\s+(TEXT|BIGINT|BOOLEAN|TIMESTAMPTZ|JSONB)(\s+NOT\s+NULL)?(\s+PRIMARY\s+KEY)?', definition, re.I)
            if not column:
                raise ValueError('Unsupported column/constraint in table ' + name)
            field, kind, not_null, primary_key = column.groups()
            columns.append({'name': field, 'type': kind.upper(), 'required': bool(not_null or primary_key)})
            if primary_key:
                keys.append(field)
        if len({c['name'] for c in columns}) != len(columns) or name in tables or not columns or not keys or not set(keys) <= {c['name'] for c in columns}:
            raise ValueError('Invalid/duplicate table or primary key: ' + name)
        for column in columns:
            column['required'] |= column['name'] in keys
        tables[name] = {'columns': columns, 'keys': keys}
    if not tables:
        raise ValueError('No supported CREATE TABLE statements found')
    return tables


def reject_constant(_):
    raise ValueError('Non-finite JSON number')


def check_json(value):
    if isinstance(value, str) and ('\x00' in value or any(0xD800 <= ord(c) <= 0xDFFF for c in value)):
        raise ValueError('JSON contains characters unsupported by PostgreSQL')
    if isinstance(value, dict):
        for key, item in value.items():
            check_json(key)
            check_json(item)
    elif isinstance(value, list):
        for item in value:
            check_json(item)


def value_text(value, column):
    if value is None:
        if column['required']:
            raise ValueError('NULL in required column')
        return None
    kind = column['type']
    if kind == 'TEXT':
        if not isinstance(value, str):
            raise ValueError('Expected text')
        return value
    if kind == 'BIGINT':
        if isinstance(value, str) and re.fullmatch(r'-?\d+', value):
            value = int(value)
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError('Expected signed 64-bit integer')
        return str(value)
    if kind == 'BOOLEAN':
        if type(value) is not bool:
            raise ValueError('Expected boolean')
        return 't' if value else 'f'
    if kind == 'TIMESTAMPTZ':
        if not isinstance(value, str):
            raise ValueError('Expected ISO timestamp with timezone')
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('Invalid ISO timestamp') from None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError('Timestamp has no timezone')
        return parsed.astimezone(timezone.utc).isoformat()
    if kind == 'JSONB':
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        try:
            parsed = json.loads(text, parse_float=Decimal, parse_constant=reject_constant)
        except (ValueError, TypeError):
            raise ValueError('Invalid JSON') from None
        check_json(parsed)
        # Retain JSON strings as JSON documents, avoiding double encoding and rounding.
        return text
    raise ValueError('Unsupported type')


def copy_field(value):
    if value is None:
        return r'\N'
    if '\x00' in value or any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError('Character unsupported by PostgreSQL text')
    return value.replace('\\', '\\\\').replace('\t', r'\t').replace('\n', r'\n').replace('\r', r'\r')


@contextmanager
def source_reader(source):
    if source.is_dir():
        files = {p.name: p for p in source.iterdir() if p.is_file()}
        yield files, lambda name: files[name].open('rb')
    else:
        with zipfile.ZipFile(source) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise ValueError('Duplicate ZIP entries')
            # No extraction, and only root-level named files are read.
            yield set(names), archive.open


def convert(source, schema_path, output, allow_partial=False):
    sql = schema_path.read_text(encoding='utf-8')
    tables = parse_schema(sql)
    if output.exists():
        raise ValueError('Output already exists; choose a new directory')
    output.parent.mkdir(parents=True, exist_ok=True)
    with source_reader(source) as (names, open_source):
        manifest = {}
        if 'manifest.json' in names:
            with open_source('manifest.json') as stream:
                manifest = json.load(stream)
        expected = {x['table']: x for x in manifest.get('tables', [])}
        missing = [name for name in tables if name + '.jsonl' not in names]
        extra = [name for name in names if name.endswith('.jsonl') and name[:-6] not in tables]
        if extra:
            raise ValueError('Data files have no target table: ' + ', '.join(sorted(extra)))
        if missing and not allow_partial:
            raise ValueError('Missing table files: ' + ', '.join(missing))
        stats = []
        with tempfile.TemporaryDirectory(prefix='.convert-', dir=output.parent) as temp:
            staging = Path(temp) / 'result'
            staging.mkdir()
            shutil.copyfile(schema_path, staging / '001_initial_schema.sql')
            with (staging / 'data.sql').open('w', encoding='utf-8', newline='\n') as target:
                target.write("\\set ON_ERROR_STOP on\nSET client_encoding = 'UTF8';\nBEGIN;\nSET LOCAL TIME ZONE 'UTC';\nSET LOCAL search_path = public;\n")
                for name, table in tables.items():
                    if name in missing:
                        continue
                    columns = table['columns']
                    fields = [c['name'] for c in columns]
                    target.write('COPY public."' + name + '" (' + ', '.join('"' + field + '"' for field in fields) + ') FROM STDIN WITH (FORMAT text);\n')
                    digest = hashlib.sha256()
                    seen = set()
                    rows = 0
                    nulls = Counter()
                    with open_source(name + '.jsonl') as stream:
                        for line_number, raw in enumerate(stream, 1):
                            digest.update(raw)
                            if not raw.strip():
                                continue
                            try:
                                row = json.loads(raw, parse_constant=reject_constant)
                                if not isinstance(row, dict) or set(row) != set(fields):
                                    raise ValueError('Row columns do not match schema')
                                values = []
                                for column in columns:
                                    try:
                                        value = value_text(row[column['name']], column)
                                        values.append(copy_field(value))
                                    except (ValueError, TypeError, OverflowError):
                                        raise ValueError('Invalid value in column ' + column['name']) from None
                                    if value is None:
                                        nulls[column['name']] += 1
                                key = tuple(values[fields.index(field)] for field in table['keys'])
                                if key in seen:
                                    raise ValueError('Duplicate primary key')
                                seen.add(key)
                            except (ValueError, UnicodeError, TypeError):
                                # Never include actual row values (tokens/customer data) in errors.
                                raise ValueError(name + ': invalid row at line ' + str(line_number)) from None
                            target.write('\t'.join(values) + '\n')
                            rows += 1
                    target.write('\\.\n')
                    metadata = expected.get(name)
                    if metadata and (rows != metadata['rows'] or digest.hexdigest() != metadata['sha256']):
                        raise ValueError('Manifest row count/checksum mismatch: ' + name)
                    stats.append({'table': name, 'rows': rows, 'sha256': digest.hexdigest(), 'manifest_verified': metadata is not None, 'nulls': dict(nulls)})
                target.write('COMMIT;\n')
            (staging / 'load.sql').write_text("\\set ON_ERROR_STOP on\nSET search_path = public;\n\\ir 001_initial_schema.sql\n\\ir data.sql\n", encoding='utf-8')
            verification = ["\\set ON_ERROR_STOP on\nDO $verify$\nBEGIN\n"]
            for item in stats:
                verification.append('  IF (SELECT count(*) FROM public."' + item['table'] + '") <> ' + str(item['rows']) + " THEN RAISE EXCEPTION 'Row count mismatch: " + item['table'] + "'; END IF;\n")
            verification.append('END;\n$verify$;\n')
            (staging / 'verify.sql').write_text(''.join(verification), encoding='utf-8')
            report = {'source': str(source), 'schema': str(schema_path), 'missing_tables': missing, 'common_snapshot_across_tables': manifest.get('common_snapshot_across_tables'), 'rows': sum(x['rows'] for x in stats), 'tables': stats}
            (staging / 'conversion-report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
            staging.rename(output)
    return report


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True, help='ZIP archive or directory with <table>.jsonl files')
    parser.add_argument('--schema', type=Path, default=Path(__file__).with_name('001_initial_schema.sql'))
    parser.add_argument('--output', type=Path, required=True, help='New output directory')
    parser.add_argument('--allow-partial', action='store_true', help='Explicitly allow missing tables')
    args = parser.parse_args()
    try:
        report = convert(args.input.resolve(), args.schema.resolve(), args.output.resolve(), args.allow_partial)
    except (ValueError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, 'Conversion failed: ' + str(error) + '\n')
    print('Converted ' + str(len(report['tables'])) + ' tables, ' + str(report['rows']) + ' rows: ' + str(args.output.resolve()))


if __name__ == '__main__':
    main()
