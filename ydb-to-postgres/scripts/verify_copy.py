#!/usr/bin/env python3
"""Stream-compare every PostgreSQL COPY field against normalized source JSONL."""
import argparse
import json
from pathlib import Path
import re
from jsonl_to_postgres import parse_schema, value_text


def decode(raw):
    if raw == r'\N':
        return None
    escapes = {'t': '\t', 'n': '\n', 'r': '\r', '\\': '\\'}
    result, index = [], 0
    while index < len(raw):
        if raw[index] == '\\':
            index += 1
            if index >= len(raw) or raw[index] not in escapes:
                raise ValueError('Unexpected COPY escape')
            result.append(escapes[raw[index]])
        else:
            result.append(raw[index])
        index += 1
    return ''.join(result)


def verify(root):
    tables = parse_schema((root / 'postgres/001_initial_schema.sql').read_text())
    report = json.loads((root / 'postgres/conversion-report.json').read_text())
    expected = {item['table']: item['rows'] for item in report['tables']}
    checked, source, name, count = {}, None, None, 0
    try:
        with (root / 'postgres/data.sql').open(encoding='utf-8') as stream:
            for line in stream:
                if source is None:
                    match = re.match(r'COPY public\."(\w+)" .* FROM STDIN WITH \(FORMAT text\);', line)
                    if not match:
                        continue
                    name = match[1]
                    if name in checked or name not in tables:
                        raise ValueError('Unexpected COPY table')
                    source = (root / 'jsonl' / (name + '.jsonl')).open(encoding='utf-8')
                    count = 0
                elif line == '\\.\n':
                    if source.read().strip() or count != expected[name]:
                        raise ValueError('COPY row count mismatch: ' + name)
                    source.close()
                    source = None
                    checked[name] = count
                else:
                    raw = source.readline()
                    if not raw:
                        raise ValueError('COPY has extra rows: ' + name)
                    row = json.loads(raw)
                    columns = tables[name]['columns']
                    actual = [decode(value) for value in line.rstrip('\n').split('\t')]
                    wanted = [value_text(row[column['name']], column) for column in columns]
                    if actual != wanted:
                        raise ValueError('COPY field mismatch: ' + name + ', row ' + str(count + 1))
                    count += 1
        if source is not None or checked != expected:
            raise ValueError('Missing/incomplete COPY section')
    finally:
        if source is not None:
            source.close()
    result = {'copy_fields_verified': True, 'rows': sum(checked.values()), 'tables': checked, 'real_postgres_import_tested': False}
    (root / 'postgres/copy-verification.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    print(json.dumps(verify(parser.parse_args().directory.resolve())))
