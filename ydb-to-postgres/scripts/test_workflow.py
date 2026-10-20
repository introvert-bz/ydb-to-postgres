#!/usr/bin/env python3
"""Offline regression tests. No cloud access or customer data."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import quote_plus
import zipfile
from jsonl_to_postgres import convert
from verify_copy import verify

SCRIPTS = Path(__file__).resolve().parent


def write_json(path, value):
    path.write_text(json.dumps(value), encoding='utf-8')


def fixture(root):
    root.mkdir()
    columns = []
    for name, kind, optional in [('id', 'UINT32', False), ('text', 'UTF8', True), ('doc', 'JSON', False), ('at', 'TIMESTAMP', False), ('flag', 'BOOL', False), ('signed', 'INT64', False)]:
        value = {'type_id': kind}
        if optional:
            value = {'optional_type': {'item': value}}
        columns.append({'name': name, 'type': value})
    rows = [
        [0, 'a + b\n\t\\N\rкириллица O\'Brien', '{"s":"+ +","n":1.234567890123456789}', '2026-01-01T01:02:03.123456Z', True, -(2**63)],
        [1, None, '{}', '2026-01-01T01:02:03Z', False, 2**63-1],
        [2, '', 'null', '2026-01-01T01:02:03Z', True, 0],
        [4294967295, 'null', '"string"', '2026-01-01T01:02:03Z', False, -1],
    ]
    folder = root / 'native-dump/items'
    folder.mkdir(parents=True)
    (folder / 'scheme.pb').write_text('\n'.join('columns { name: "' + c['name'] + '" }' for c in columns))
    data = []
    for row in rows:
        tokens = []
        for value, column in zip(row, columns):
            if value is None:
                tokens.append('null')
            elif column['name'] in ('text', 'doc'):
                tokens.append('"' + quote_plus(value, safe='') + '"')
            elif type(value) is bool:
                tokens.append('true' if value else 'false')
            else:
                tokens.append(str(value))
        data.append(','.join(tokens))
    (folder / 'data_00.csv').write_text('\n'.join(data) + '\n')
    write_json(root / 'objects.json', [{'type': 'table', 'path': 'items'}])
    write_json(root / 'items.schema.json', {'columns': columns, 'primary_key': ['id'], 'indexes': [{'name': 'by_flag', 'index_columns': ['flag'], 'global_index': {}}]})
    write_json(root / 'database.json', {'database': '/Root/synthetic'})
    write_json(root / 'source-counts.json', {'items': {'rows': 4, 'verified': True}})
    write_json(root / 'download-manifest.json', {'database': '/Root/synthetic', 'export_date_moscow': '2026-01-01', 'tables': [{'table': 'items', 'rows': 4}]})
    return rows


def call(script, *args):
    result = subprocess.run([sys.executable, str(SCRIPTS / script), *map(str, args)], capture_output=True, text=True)
    if result.returncode:
        raise AssertionError(result.stderr)
    return result


class WorkflowTests(unittest.TestCase):
    def test_roundtrip_archives_and_corruption_detection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'export'
            expected = fixture(root)
            native = (root / 'native-dump/items/data_00.csv').read_bytes()
            call('package_copy.py', 'raw', root)
            call('native_to_postgres.py', root, '--database', '/Root/synthetic')
            actual = [json.loads(line) for line in (root / 'jsonl/items.jsonl').read_text().splitlines()]
            self.assertEqual([row['text'] for row in actual], [row[1] for row in expected])
            self.assertEqual([row['signed'] for row in actual], [row[5] for row in expected])
            self.assertEqual(actual[0]['at'], '2026-01-01T01:02:03.123456+00:00')
            self.assertEqual(actual[0]['doc'], expected[0][2])
            self.assertEqual(verify(root)['rows'], 4)
            call('package_copy.py', 'postgres', root)
            for suffix in ('ydb-original', 'postgresql'):
                with zipfile.ZipFile(Path(temp) / ('export-' + suffix + '.zip')) as archive:
                    self.assertIsNone(archive.testzip())
                    if suffix == 'ydb-original':
                        self.assertEqual(archive.read('native-dump/items/data_00.csv'), native)
                    else:
                        sql = archive.read('postgres/pgadmin/01_items_001.sql').decode()
                        self.assertIn("O''Brien", sql)
                        self.assertIn("NULL", sql)
                        self.assertNotIn('COPY ', sql)
            path = root / 'postgres/data.sql'
            path.write_text(path.read_text().replace('a + b', 'CORRUPTED', 1))
            with self.assertRaises(ValueError):
                verify(root)

    def test_downloader_with_fake_cli(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            fixture(base / 'fixture')
            fake = base / 'fake-cli'
            fake.write_text('#!' + sys.executable + '\n' + r"""
import json, os, shutil, sys
from pathlib import Path
args = sys.argv[1:]
fixture = Path(os.environ['YDB_TEST_FIXTURE'])
with (fixture.parent / 'calls.jsonl').open('a') as stream:
    stream.write(json.dumps(args) + '\n')
if 'create-token' in args:
    print('SYNTHETIC_OFFLINE_TOKEN')
elif 'get' in args:
    print('{}')
elif 'ls' in args:
    print((fixture / 'objects.json').read_text())
elif 'describe' in args:
    print((fixture / 'items.schema.json').read_text())
elif 'dump' in args:
    assert '--avoid-copy' in args and '--save-partial-result' in args
    shutil.copytree(fixture / 'native-dump', args[args.index('--output') + 1])
elif 'execute' in args:
    assert '--type' in args and args[args.index('--type') + 1] == 'scan'
    print('{"row_count":4}')
else:
    sys.exit(2)
""")
            fake.chmod(0o700)
            env = dict(os.environ, YDB_TEST_FIXTURE=str(base / 'fixture'))
            result = subprocess.run([sys.executable, str(SCRIPTS / 'download_ydb.py'), '--database', '/Root/synthetic', '--endpoint', 'grpcs://offline.invalid:2135', '--profile', 'offline', '--yc-bin', str(fake), '--ydb-bin', str(fake), '--output', str(base / 'result')], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            counts = json.loads((base / 'result/source-counts.json').read_text())
            self.assertEqual(counts['items'], {'rows': 4, 'verified': True, 'matches_export': True})
            calls = [json.loads(line) for line in (base / 'calls.jsonl').read_text().splitlines()]
            token_paths = {args[args.index('--iam-token-file') + 1] for args in calls if '--iam-token-file' in args}
            self.assertTrue(token_paths)
            self.assertTrue(all(not Path(path).exists() for path in token_paths))
            self.assertNotIn('SYNTHETIC_OFFLINE_TOKEN', ''.join(p.read_text() for p in (base / 'result').rglob('*') if p.is_file()))

    def test_atomic_duplicate_key_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'export'
            fixture(root)
            call('native_to_postgres.py', root, '--database', '/Root/synthetic')
            source = root / 'jsonl/items.jsonl'
            with source.open('a') as stream:
                stream.write(source.read_text().splitlines()[0] + '\n')
            target = root / 'invalid-postgres'
            with self.assertRaises(ValueError):
                convert(root / 'jsonl', root / '001_initial_schema.sql', target)
            self.assertFalse(target.exists())

    def test_unsupported_type_keeps_native_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'export'
            fixture(root)
            path = root / 'items.schema.json'
            schema = json.loads(path.read_text())
            schema['columns'][0]['type'] = {'type_id': 'UINT64'}
            write_json(path, schema)
            call('package_copy.py', 'raw', root)
            result = subprocess.run([sys.executable, str(SCRIPTS / 'native_to_postgres.py'), str(root), '--database', '/Root/synthetic'], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'postgres').exists())
            self.assertTrue((Path(temp) / 'export-ydb-original.zip').exists())


if __name__ == '__main__':
    unittest.main()
