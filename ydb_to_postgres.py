#!/usr/bin/env python3
"""Standalone YDB download/conversion CLI. Python 3.9+, no AI required."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

REPO = Path(__file__).resolve().parent
SCRIPTS = REPO / 'ydb-to-postgres' / 'scripts'


def run(script, *args):
    subprocess.run([sys.executable, str(SCRIPTS / script), *map(str, args)], check=True)


def convert(root, database):
    raw = root.parent / (root.name + '-ydb-original.zip')
    if not raw.exists():
        run('package_copy.py', 'raw', root)
    else:
        # Do not silently trust an unrelated/stale archive during a retry.
        raise ValueError('The native archive already exists. Use a fresh directory with the original download files.')
    print('Native archive saved. Conversion starts; a failure preserves the archive.', flush=True)
    run('native_to_postgres.py', root, '--database', database)
    run('verify_copy.py', root)
    run('package_copy.py', 'postgres', root)
    counts = json.loads((root / 'source-counts.json').read_text())
    verified = all(item.get('verified') for item in counts.values())
    print(json.dumps({'native_archive': str(raw), 'postgresql_archive': str(root.parent / (root.name + '-postgresql.zip')), 'source_counts_verified': verified, 'real_postgres_import_tested': False}, ensure_ascii=False), flush=True)
    if not verified:
        print('Archives are ready, but independent source counts are NOT fully verified. See source-counts.json.', file=sys.stderr)
        return 2
    return 0


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    export = commands.add_parser('export', help='Download YDB and create both ZIP archives')
    export.add_argument('--database', required=True, help='Full source database path')
    export.add_argument('--endpoint', required=True, help='YDB endpoint')
    export.add_argument('--output', type=Path, required=True, help='New private download directory; ZIPs are saved alongside it')
    auth = export.add_mutually_exclusive_group(required=True)
    auth.add_argument('--profile', help='Existing authenticated YC profile')
    auth.add_argument('--iam-token-file', type=Path, help='Existing IAM token file')
    export.add_argument('--yc-bin', default='yc')
    export.add_argument('--ydb-bin', default='ydb')
    export.add_argument('--dump-timeout', type=int, default=7200)
    export.add_argument('--count-timeout', type=int, default=90)
    existing = commands.add_parser('convert', help='Package/convert a completed download directory created by this toolkit')
    existing.add_argument('--input', type=Path, required=True)
    existing.add_argument('--database', required=True)
    demo = commands.add_parser('demo', help='Generate the fully invented four-table example offline')
    demo.add_argument('--output', type=Path, required=True, help='New output directory')
    args = parser.parse_args()
    try:
        if args.command == 'demo':
            subprocess.run([sys.executable, str(REPO / 'tools/build_examples.py'), '--output', str(args.output)], check=True)
            return 0
        if args.command == 'export':
            if args.output.exists():
                raise ValueError('Output directory already exists; choose a new directory.')
            if args.dump_timeout <= 0 or args.count_timeout <= 0:
                raise ValueError('Timeouts must be positive.')
            for suffix in ('-ydb-original.zip', '-postgresql.zip'):
                if args.output.parent.joinpath(args.output.name + suffix).exists():
                    raise ValueError('An output archive already exists; choose a new output name.')
            if args.iam_token_file is not None and not args.iam_token_file.is_file():
                raise ValueError('IAM token file does not exist.')
            if not shutil.which(args.ydb_bin):
                raise ValueError('YDB CLI not found. Install it or pass --ydb-bin.')
            if args.profile and not shutil.which(args.yc_bin):
                raise ValueError('YC CLI not found. Install it or use --iam-token-file.')
            sys.path.insert(0, str(SCRIPTS))
            from download_ydb import download
            download(args)
            return convert(args.output.resolve(), args.database)
        return convert(args.input.resolve(), args.database)
    except subprocess.CalledProcessError:
        print('A pipeline stage failed. Existing files and any native archive were retained. Do not treat partial files as a complete backup.', file=sys.stderr)
    except (ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
    except (OSError, subprocess.TimeoutExpired):
        print('An I/O error or timeout stopped the operation. Retain the files for diagnosis.', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
