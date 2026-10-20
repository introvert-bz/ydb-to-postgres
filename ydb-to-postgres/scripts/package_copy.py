#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import zipfile


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def raw_archive(root):
    tables = json.loads((root / 'download-manifest.json').read_text())['tables']
    counts = json.loads((root / 'source-counts.json').read_text())
    for table in tables:
        if counts.get(table['table'], {}).get('verified'):
            assert counts[table['table']]['rows'] == table['rows'], 'Source counts changed: ' + table['table']
    native_files = [p for p in sorted((root / 'native-dump').rglob('*')) if p.is_file() and '.sys' not in p.relative_to(root).parts]
    assert not any(p.name.startswith('incomplete') for p in native_files)
    metadata = [root / 'database.json', root / 'objects.json', root / 'source-counts.json']
    metadata += [root / (table['table'] + '.schema.json') for table in tables]
    manifest = {'database': json.loads((root / 'download-manifest.json').read_text())['database'], 'export_date_moscow': json.loads((root / 'download-manifest.json').read_text())['export_date_moscow'], 'format': 'Native YDB CLI tools dump', 'native_dump_complete': True, 'system_objects_excluded': True, 'common_snapshot_across_tables': False, 'source_count_verification': counts, 'tables': [{'table': t['table'], 'rows': t['rows']} for t in tables], 'files': [{'path': str(p.relative_to(root)), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in native_files + metadata]}
    (root / 'native-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    readme = """Исходный штатный дамп YDB. Схема: native-dump/<table>/scheme.pb.
Данные data_XX.csv сохранены без изменений. Описания таблиц из API — *.schema.json.
Количество строк, проверки и источник перечислены в native-manifest.json.
Системные объекты .sys исключены. Дамп --avoid-copy не гарантирует общего снимка.
Восстанавливайте в отдельную пустую YDB, подставив свои параметры:
ydb -e ENDPOINT -d TARGET_DATABASE --iam-token-file TOKEN_FILE tools restore --path TARGET_DATABASE --input /path/native-dump --restore-acl 0
Не восстанавливайте в исходную производственную базу без отдельного разрешения.
"""
    archive = root.parent / (root.name + '-ydb-original.zip')
    if archive.exists():
        raise ValueError('Archive already exists; choose a new output name')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as target:
        for path in native_files + metadata + [root / 'native-manifest.json']:
            target.write(path, str(path.relative_to(root)))
        target.writestr('README.txt', readme)
    with zipfile.ZipFile(archive) as target:
        assert target.testzip() is None
    print(json.dumps({'archive': str(archive), 'bytes': archive.stat().st_size, 'sha256': sha256(archive)}), flush=True)


def postgres_archive(root):
    from verify_copy import verify
    verify(root)
    postgres = root / 'postgres'
    report = json.loads((postgres / 'conversion-report.json').read_text())
    counts = json.loads((root / 'download-manifest.json').read_text())
    assert report['rows'] == sum(t['rows'] for t in counts['tables'])
    assert {t['table'] for t in report['tables']} == {t['table'] for t in counts['tables']}
    assert all(t['manifest_verified'] for t in report['tables'])
    from jsonl_to_postgres import parse_schema, value_text
    tables = parse_schema((postgres / '001_initial_schema.sql').read_text())
    query_tool = postgres / 'pgadmin'
    query_tool.mkdir(exist_ok=False)
    # Fully ordinary SQL files: no psql directives, COPY STDIN or filesystem paths.
    shutil.copyfile(postgres / '001_initial_schema.sql', query_tool / '00_schema.sql')
    max_bytes = 20 * 1024 * 1024
    for table_number, (name, table) in enumerate(tables.items(), 1):
        batch, stream, chunk_size, part, count = [], None, 0, 0, 0
        header = 'INSERT INTO public."' + name + '" (' + ', '.join('"' + c['name'] + '"' for c in table['columns']) + ') VALUES\n'
        def flush():
            nonlocal stream, chunk_size, part
            if not batch:
                return
            text = header + ',\n'.join(batch) + ';\n'
            if stream is None or chunk_size + len(text.encode('utf-8')) > max_bytes:
                if stream is not None:
                    stream.write('COMMIT;\n')
                    stream.close()
                part += 1
                path = query_tool / (str(table_number).zfill(2) + '_' + name + '_' + str(part).zfill(3) + '.sql')
                stream = path.open('w', encoding='utf-8')
                stream.write("BEGIN;\nSET LOCAL standard_conforming_strings = on;\nSET LOCAL TIME ZONE 'UTC';\n")
                chunk_size = 100
            stream.write(text)
            chunk_size += len(text.encode('utf-8'))
            batch.clear()
        with (root / 'jsonl' / (name + '.jsonl')).open() as source:
            for line in source:
                if not line.strip():
                    continue
                row = json.loads(line)
                literals = []
                for column in table['columns']:
                    value = value_text(row[column['name']], column)
                    if value is None:
                        literal = 'NULL'
                    elif column['type'] == 'BIGINT':
                        literal = value
                    elif column['type'] == 'BOOLEAN':
                        literal = 'TRUE' if value == 't' else 'FALSE'
                    else:
                        literal = "'" + value.replace("'", "''") + "'"
                    literals.append(literal)
                batch.append('(' + ', '.join(literals) + ')')
                count += 1
                if len(batch) >= 200:
                    flush()
        flush()
        if stream is not None:
            stream.write('COMMIT;\n')
            stream.close()
        assert count == next(t['rows'] for t in report['tables'] if t['table'] == name)
        print(json.dumps({'pgadmin_table': name, 'rows': count, 'parts': part}), flush=True)
    verification = '\n'.join(line for line in (postgres / 'verify.sql').read_text().splitlines() if not line.startswith('\\'))
    (query_tool / '99_verify.sql').write_text(verification + '\n')
    shutil.copyfile(root / 'schema-mapping.json', postgres / 'schema-mapping.json')
    shutil.copyfile(root / 'source-counts.json', postgres / 'source-counts.json')
    tools = postgres / 'tools'
    tools.mkdir()
    for script in ['native_to_postgres.py', 'jsonl_to_postgres.py']:
        shutil.copyfile(Path(__file__).with_name(script), tools / script)
    readme = """Загрузка сконвертированной YDB в PostgreSQL
Подготовьте отдельную пустую базу: совпадающие имена таблиц могут иметь другую схему.
1. Через psql (быстрее; единая транзакция данных):
psql -X --dbname=TARGET_DATABASE --set=ON_ERROR_STOP=1 --file=/path/postgres/load.sql
psql -X --dbname=TARGET_DATABASE --set=ON_ERROR_STOP=1 --file=/path/postgres/verify.sql
2. Через обычный Query Tool в pgAdmin:
Выполните pgadmin/00_schema.sql, затем остальные части по имени, затем 99_verify.sql.
Каждый файл выполняйте целиком. Большие таблицы разбиты на части около 20 МБ.
Каждая часть фиксируется отдельно: при сбое ранее успешные части останутся в базе.
После ошибки выполните ROLLBACK; и продолжайте с первой НЕУСПЕШНОЙ части.
Не выполняйте повторно успешно загруженные части и не смешивайте два способа.
Проверка выдаст DO без ошибок, если число строк совпало с экспортом.

JSONB нормализует формат JSON. UTC-даты сохранены как TIMESTAMPTZ.
Точное сопоставление типов, ключей и индексов — schema-mapping.json.
Числа строк и исходные контрольные суммы — conversion-report.json.
Дамп с --avoid-copy не гарантирует единый согласованный момент всех таблиц.
Реальный импорт в PostgreSQL здесь не выполнялся.
"""
    (postgres / 'README.txt').write_text(readme, encoding='utf-8')
    files = [p for p in sorted(postgres.rglob('*')) if p.is_file() and '__pycache__' not in p.parts]
    manifest = {'database': json.loads((root / 'download-manifest.json').read_text())['database'], 'source_export': report.get('source'), 'rows': report['rows'], 'tables': [{'table': t['table'], 'rows': t['rows']} for t in report['tables']], 'common_snapshot_across_tables': False, 'files': [{'path': str(p.relative_to(postgres)), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in files]}
    (postgres / 'files-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    archive = root.parent / (root.name + '-postgresql.zip')
    if archive.exists():
        raise ValueError('Archive already exists; choose a new output name')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as target:
        for path in files + [postgres / 'files-manifest.json']:
            target.write(path, 'postgres/' + str(path.relative_to(postgres)))
    with zipfile.ZipFile(archive) as target:
        assert target.testzip() is None
    print(json.dumps({'archive': str(archive), 'bytes': archive.stat().st_size, 'sha256': sha256(archive)}), flush=True)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['raw', 'postgres'])
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    (raw_archive if args.mode == 'raw' else postgres_archive)(args.directory.resolve())
