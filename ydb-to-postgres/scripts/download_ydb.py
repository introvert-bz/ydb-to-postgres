#!/usr/bin/env python3
"""Read-only YDB download using a user IAM token; never prints credentials."""
import argparse
import csv
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def run(command, timeout=90):
    result = subprocess.run(command, capture_output=True, timeout=timeout)
    if result.returncode:
        # Do not echo subprocess stderr: authentication errors can contain secrets.
        raise RuntimeError('Command failed (exit %s); check authentication, permissions and CLI version' % result.returncode)
    return result.stdout


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def download(args):
    os.umask(0o077)
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    started = datetime.now(timezone.utc).isoformat()
    yc = [args.yc_bin] + (['--profile', args.profile] if args.profile else []) + ['--no-browser']
    with tempfile.TemporaryDirectory(prefix='ydb-auth-') as temp:
        token = args.iam_token_file
        if token is None:
            token = Path(temp) / 'iam-token'
            token.write_bytes(run(yc + ['iam', 'create-token']).strip() + b'\n')
            token.chmod(0o600)
        base = [args.ydb_bin, '-e', args.endpoint, '-d', args.database, '--iam-token-file', str(token)]
        # Metadata is optional if user can read YDB but cannot inspect cloud resources.
        try:
            metadata = json.loads(run(yc + ['ydb', 'database', 'get', '--id', args.database.rstrip('/').split('/')[-1], '--format', 'json']))
        except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError):
            metadata = {'database': args.database, 'endpoint': args.endpoint, 'cloud_metadata_available': False}
        write_json(root / 'database.json', metadata)
        objects = json.loads(run(base + ['scheme', 'ls', '--timeout', '30s', '-R', '-l', '--format', 'json']))
        write_json(root / 'objects.json', objects)
        tables = sorted(item['path'] for item in objects if item['type'] == 'table' and not item['path'].startswith('.sys/'))
        if not tables:
            raise ValueError('No tables discovered; do not interpret a metadata failure as an empty database')
        # Nested paths and unusual identifiers need an explicit, reviewed mapping.
        if any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', name) for name in tables):
            raise ValueError('Nested/unusual table paths require an explicit mapping before downloading')
        for name in tables:
            schema = json.loads(run(base + ['scheme', 'describe', '--timeout', '30s', '--format', 'proto-json-base64', name]))
            write_json(root / (name + '.schema.json'), schema)
        with (root / 'dump.log').open('wb') as log:
            result = subprocess.run(base + ['tools', 'dump', '--path', '.', '--output', str(root / 'native-dump'), '--avoid-copy', '--ordered', '--save-partial-result', '--exclude', r'^\.sys(/|$)'], stdout=log, stderr=log, timeout=args.dump_timeout)
        if result.returncode:
            raise RuntimeError('Dump failed; preserved partial files are NOT a complete backup')
        native = root / 'native-dump'
        if list(native.rglob('incomplete*')):
            raise ValueError('Dump contains incomplete markers; not a complete backup')
        counts, downloaded = {}, []
        for name in tables:
            files = sorted((native / name).glob('data_*.csv'))
            if not files:
                raise ValueError('Missing data files for ' + name)
            rows = 0
            for path in files:
                with path.open(encoding='utf-8', newline='') as stream:
                    rows += sum(1 for _ in csv.reader(stream, quoting=csv.QUOTE_NONE))
            downloaded.append({'table': name, 'rows': rows})
            try:
                raw = run(base + ['table', 'query', 'execute', '--type', 'scan', '--format', 'json-base64', '-q', 'SELECT COUNT(*) AS row_count FROM `' + name + '`;'], timeout=args.count_timeout)
                values = [json.loads(line) for line in raw.decode().splitlines() if line.strip()]
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError('Unexpected count output')
                count = int(values[0]['row_count'])
                counts[name] = {'rows': count, 'verified': count == rows, 'matches_export': count == rows}
            except (RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
                counts[name] = {'verified': False, 'error': 'Independent count unavailable; not assumed zero'}
            print(json.dumps({'table': name, 'downloaded_rows': rows, 'source_verified': counts[name]['verified']}), flush=True)
        write_json(root / 'source-counts.json', counts)
        write_json(root / 'download-manifest.json', {'database': args.database, 'endpoint': args.endpoint, 'started_at_utc': started, 'finished_at_utc': datetime.now(timezone.utc).isoformat(), 'export_date_moscow': datetime.now(timezone(timedelta(hours=3))).date().isoformat(), 'common_snapshot_across_tables': False, 'tables': downloaded})
        if not all(value['verified'] for value in counts.values()):
            print('Native download completed; source counts are NOT fully verified. Inspect source-counts.json.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--output', type=Path, required=True, help='New private directory')
    parser.add_argument('--profile', help='Existing YC user profile')
    parser.add_argument('--yc-bin', default='yc')
    parser.add_argument('--ydb-bin', default='ydb')
    parser.add_argument('--iam-token-file', type=Path, help='Existing IAM token file; never an OAuth token')
    parser.add_argument('--dump-timeout', type=int, default=7200)
    parser.add_argument('--count-timeout', type=int, default=90)
    args = parser.parse_args()
    try:
        download(args)
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as error:
        parser.exit(1, type(error).__name__ + ': export did not complete; retain files for diagnosis, do not call them a full backup.\n')


if __name__ == '__main__':
    main()
