from pathlib import Path
import shutil,json,subprocess,sys,hashlib,zipfile
from urllib.parse import quote_plus
import argparse
parser=argparse.ArgumentParser(description='Build a fully invented four-table YDB-format fixture and PostgreSQL conversion, offline.')
parser.add_argument('--output',type=Path,required=True,help='New output directory')
args=parser.parse_args()
repo=Path(__file__).resolve().parents[1]
skill=repo/'ydb-to-postgres'
sys.path.insert(0, str(skill/'scripts'))
from native_io import native_records
output=args.output.resolve()
output.mkdir(parents=True,exist_ok=False)
root=output/'shop-demo'
root.mkdir()

def write(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')

specs={
'customers': [('id','UINT32',False),('name','UTF8',False),('email','UTF8',True),('created_at','TIMESTAMP',False)],
'products': [('id','UINT32',False),('title','UTF8',False),('price_minor','INT64',False),('active','BOOL',False),('attributes','JSON_DOCUMENT',False)],
'orders': [('id','UINT32',False),('customer_id','UINT32',False),('status','UTF8',False),('placed_at','DATETIME',False),('note','UTF8',True)],
'order_items': [('order_id','UINT32',False),('product_id','UINT32',False),('quantity','UINT32',False),('unit_price_minor','INT64',False)]}
rows={
'customers': [[1,'Покупатель Альфа','alpha@example.invalid','2026-01-01T10:00:00.123456Z'],[2,'Покупатель Бета',None,'2026-01-02T11:00:00Z'],[3,"Demo O'Brien",'demo@example.invalid','2026-01-03T12:00:00Z']],
'products': [[101,'Блокнот A + B',12500,True,'{"color":"blue","tags":["paper","A+B"]}'],[102,'Кружка',89000,True,'{"volume_ml":350}'],[103,'Подарочный пакет',5000,False,'{}'],[104,'Наклейка',0,True,'{"text":"Строка\\nс переносом"}']],
'orders': [[1001,1,'paid','2026-02-01T10:15:00Z',None],[1002,2,'draft','2026-02-02T12:00:00Z',''],[1003,3,'shipped','2026-02-03T15:30:00Z',"null"],[1004,1,'paid','2026-02-04T09:00:00Z',"A + B\nВторая строка\tO'Brien\\N"]],
'order_items': [[1001,101,2,12500],[1001,102,1,89000],[1002,103,3,5000],[1003,102,1,89000],[1004,104,10,0],[1004,101,1,12500]]}
for table,spec in specs.items():
    cols=[]; pb=[]
    for name,kind,opt in spec:
        typ={'type_id':kind}
        if opt: typ={'optional_type':{'item':typ}}
        cols.append({'name':name,'type':typ})
        ptype=('optional_type { item { type_id: '+kind+' } }') if opt else 'type_id: '+kind
        pb.append('columns { name: "'+name+'" type { '+ptype+' } }')
    keys=['order_id','product_id'] if table=='order_items' else ['id']
    indexes=[]
    if table=='orders': indexes=[{'name':'by_customer','index_columns':['customer_id'],'global_index':{}}]
    for key in keys: pb.append('primary_key: "'+key+'"')
    if indexes: pb.append('indexes { name: "by_customer" index_columns: "customer_id" global_index {} }')
    write(root/(table+'.schema.json'),{'columns':cols,'primary_key':keys,'indexes':indexes})
    folder=root/'native-dump'/table
    folder.mkdir(parents=True)
    (folder/'scheme.pb').write_text('\n'.join(pb)+'\n')
    encoded=[]
    for row in rows[table]:
        cells=[]
        for value,(_,kind,_) in zip(row,spec):
            if value is None: cells.append('null')
            elif kind in ('UTF8','JSON','JSON_DOCUMENT'): cells.append('"'+quote_plus(value,safe='')+'"')
            elif kind=='BOOL': cells.append(str(value).lower())
            else: cells.append(str(value))
        encoded.append(','.join(cells))
    (folder/'data_00.csv').write_text('\n'.join(encoded)+'\n')
write(root/'objects.json',[{'type':'table','path':name} for name in sorted(specs)])
write(root/'database.json',{'database':'/Root/shop_demo','synthetic':True,'description':'Fully invented schema and rows. No cloud account or production data.'})
write(root/'source-counts.json',{name:{'rows':len(value),'verified':True,'verification_method':'offline synthetic fixture; not a live YDB COUNT'} for name,value in rows.items()})
write(root/'download-manifest.json',{'database':'/Root/shop_demo','export_date_moscow':'2026-01-01','synthetic':True,'tables':[{'table':name,'rows':len(value)} for name,value in sorted(rows.items())]})

def call(script,*args): subprocess.run([sys.executable,str(skill/'scripts'/script),*map(str,args)],check=True)
manifest_path = root/'download-manifest.json'
manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
manifest['native_files'] = native_records(root)
write(manifest_path, manifest)
call('package_copy.py','raw',root)
call('native_to_postgres.py',root,'--database','/Root/shop_demo')
# Published reports use portable paths rather than workstation paths.
report_path=root/'postgres/conversion-report.json'
report=json.loads(report_path.read_text()); report['source']='jsonl'; report['schema']='001_initial_schema.sql'
write(report_path,report)
call('verify_copy.py',root)
# Verify normalized rows against the in-memory, invented source values.
from datetime import datetime,timezone
for table,spec in specs.items():
    expected=[]
    for values in rows[table]:
        row={}
        for value,(name,kind,opt) in zip(values,spec):
            if value is not None and kind in ('TIMESTAMP','DATETIME'): value=datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(timezone.utc).isoformat()
            row[name]=value
        expected.append(row)
    actual=[json.loads(line) for line in (root/'jsonl'/(table+'.jsonl')).read_text().splitlines()]
    assert actual==expected,table
call('package_copy.py','postgres',root)
examples=output
# The packager's generic README assumes a cloud dump; replace it with truthful fixture provenance.
p=examples/'shop-demo-ydb-original.zip'
with zipfile.ZipFile(p) as z: content={n:z.read(n) for n in z.namelist()}
content['README.txt']=('Полностью выдуманная учебная БД shop_demo: 4 таблицы, 17 строк.\nСхема и строки созданы локально, это НЕ выгрузка из живой YDB.\nФормат файлов соответствует входному формату конвертера: scheme.pb и data_00.csv.\nВосстановление в YDB не тестировалось. COUNT проверен по генератору, не запросом к серверу.\nНикакие исходные клиентские данные, ACL, идентификаторы облака или ключи не использовались.\n').encode()
with zipfile.ZipFile(p,'w',zipfile.ZIP_DEFLATED) as z:
    for n,data in content.items(): z.writestr(n,data)
for p in examples.glob('*.zip'):
    with zipfile.ZipFile(p) as z: assert z.testzip() is None
(examples/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in sorted(examples.glob('*.zip'))))
print('PUBLICATION READY',sum(map(len,rows.values())),'rows')
