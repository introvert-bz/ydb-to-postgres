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
from native_io import count_rows, native_records


def run(command, timeout=90, *, stage):
    try:
        result = subprocess.run(command, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError('Stage ' + stage + ': timed out; check connectivity and timeouts.') from None
    except OSError:
        raise RuntimeError('Stage ' + stage + ': CLI executable is unavailable; check --ydb-bin/--yc-bin.') from None
    if result.returncode:
        # Never echo the command, stdout or stderr: they can contain credentials.
        raise RuntimeError('Stage ' + stage + ': command failed (exit %s); check authentication, permissions and CLI version.' % result.returncode)
    return result.stdout


def run_json(command, *, stage):
    raw = run(command, stage=stage)
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise RuntimeError('Stage ' + stage + ': unexpected CLI JSON format; check the supported CLI versions.') from None


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
            token.write_bytes(run(yc + ['iam', 'create-token'], stage='authentication').strip() + b'\n')
            token.chmod(0o600)
        base = [args.ydb_bin, '-e', args.endpoint, '-d', args.database, '--iam-token-file', str(token)]
        # Metadata is optional if user can read YDB but cannot inspect cloud resources.
        try:
            metadata = run_json(yc + ['ydb', 'database', 'get', '--id', args.database.rstrip('/').split('/')[-1], '--format', 'json'], stage='cloud_metadata')
        except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError):
            metadata = {'database': args.database, 'endpoint': args.endpoint, 'cloud_metadata_available': False}
        write_json(root / 'database.json', metadata)
        objects = run_json(base + ['scheme', 'ls', '--timeout', '30s', '-R', '-l', '--format', 'json'], stage='discovery')
        write_json(root / 'objects.json', objects)
        tables = sorted(item['path'] for item in objects if item['type'] == 'table' and not item['path'].startswith('.sys/'))
        if not tables:
            raise ValueError('No tables discovered; do not interpret a metadata failure as an empty database')
        # Nested paths and unusual identifiers need an explicit, reviewed mapping.
        if any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', name) for name in tables):
            raise ValueError('Nested/unusual table paths require an explicit mapping before downloading')
        for name in tables:
            schema = run_json(base + ['scheme', 'describe', '--timeout', '30s', '--format', 'proto-json-base64', name], stage='schema:' + name)
            write_json(root / (name + '.schema.json'), schema)
        try:
            with (root / 'dump.log').open('wb') as log:
                result = subprocess.run(base + ['tools', 'dump', '--path', '.', '--output', str(root / 'native-dump'), '--avoid-copy', '--ordered', '--save-partial-result', '--exclude', r'^\.sys(/|$)'], stdout=log, stderr=log, timeout=args.dump_timeout)
        except subprocess.TimeoutExpired:
            raise RuntimeError('Stage dump: timed out; inspect the private dump.log. Partial files are not a complete backup.') from None
        except OSError:
            raise RuntimeError('Stage dump: cannot execute YDB CLI or write dump.log.') from None
        if result.returncode:
            raise RuntimeError('Stage dump: command failed (exit %s); inspect the private dump.log. Partial files are not a complete backup.' % result.returncode)
        native = root / 'native-dump'
        if list(native.rglob('incomplete*')):
            raise ValueError('Dump contains incomplete markers; not a complete backup')
        counts, downloaded = {}, []
        for name in tables:
            files = sorted((native / name).glob('data_*.csv'))
            if not files:
                raise ValueError('Missing data files for ' + name)
            columns = json.loads((root / (name + '.schema.json')).read_text(encoding='utf-8'))['columns']
            rows = sum(count_rows(path, len(columns)) for path in files)
            downloaded.append({'table': name, 'rows': rows})
            try:
                raw = run(base + ['table', 'query', 'execute', '--type', 'scan', '--format', 'json-base64', '-q', 'SELECT COUNT(*) AS row_count FROM `' + name + '`;'], timeout=args.count_timeout, stage='count:' + name)
                values = [json.loads(line) for line in raw.decode().splitlines() if line.strip()]
                if len(values) != 1 or not isinstance(values[0], dict):
                    raise ValueError('Unexpected count output')
                count = int(values[0]['row_count'])
                counts[name] = {'rows': count, 'verified': count == rows, 'matches_export': count == rows}
            except (RuntimeError, subprocess.TimeoutExpired, ValueError, KeyError):
                counts[name] = {'verified': False, 'error': 'Independent count unavailable; not assumed zero'}
            print(json.dumps({'table': name, 'downloaded_rows': rows, 'source_verified': counts[name]['verified']}), flush=True)
        write_json(root / 'source-counts.json', counts)
        write_json(root / 'download-manifest.json', {'database': args.database, 'endpoint': args.endpoint, 'started_at_utc': started, 'finished_at_utc': datetime.now(timezone.utc).isoformat(), 'export_date_moscow': datetime.now(timezone(timedelta(hours=3))).date().isoformat(), 'common_snapshot_across_tables': False, 'tables': downloaded, 'native_files': native_records(root)})
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
    except RuntimeError as error:
        parser.exit(1, str(error) + '\n')
    except (ValueError, OSError, subprocess.TimeoutExpired) as error:
        parser.exit(1, type(error).__name__ + ': export did not complete; retain files for diagnosis, do not call them a full backup.\n')


if __name__ == '__main__':
    main()
