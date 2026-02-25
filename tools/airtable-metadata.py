import urllib.request
import json
import re
import pandas as pd
from pathlib import Path

PAT = 'REDACTED_AIRTABLE_TOKEN'
BASE_ID = 'appjwOgR4HsXeGIda'

def make_request(method, url, data=None, headers=None):
    if headers is None:
        headers = {}
    headers['Authorization'] = f'Bearer {PAT}'
    headers['Content-Type'] = 'application/json'
    
    req = urllib.request.Request(url, method=method)
    for key, value in headers.items():
        req.add_header(key, value)
    
    if data:
        req.data = json.dumps(data).encode('utf-8')
    
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        raise Exception(f"HTTP Error {e.code}: {error_body}")

# Fetch base schema with include=visibleFieldIds
schema_url = f"https://api.airtable.com/v0/meta/bases/{BASE_ID}/tables?include=visibleFieldIds"
schema = make_request('GET', schema_url)
tables = schema['tables']

# Collect unique EDI view names
edi_view_names = set()
for t in tables:
    for v in t.get('views', []):
        if 'EDI' in v['name']:
            edi_view_names.add(v['name'])

sorted_edi_views = sorted(edi_view_names)

# Settings (for field names in the export)
settings = {
    'table_id_field': 'Table ID',
    'table_name_field': 'Table Name',
    'field_id_field': 'Field ID',
    'field_name_field': 'Field Name',
    'field_description_field': 'Field Description',
    'field_type_field': 'Field Type',
    'field_options_field': 'Field Options',
    'field_camelcase_name_field': 'Field CamelCase Name',
    'edi_field': 'EDI',
}

def get_field_options(field):
    if field['type'] in ['singleSelect', 'multipleSelects']:
        return ', '.join([choice['name'] for choice in field.get('options', {}).get('choices', [])])
    elif field['type'] == 'multipleRecordLinks':
        linked_table = next((t for t in tables if t['id'] == field.get('options', {}).get('linkedTableId')), None)
        return linked_table['name'] if linked_table else None
    return None

def to_camel_case(s):
    words = re.findall(r'[a-zA-Z0-9]+', s) or []
    if not words:
        return ''
    return words[0].lower() + ''.join(word.capitalize() for word in words[1:])

# Collect metadata for all tables (no filtering)
metadata_entries = []

for t in tables:
    edi_views = [v for v in t.get('views', []) if 'EDI' in v['name']]
    edi_views_dict = {v['name']: v for v in edi_views}
    for f in t['fields']:
        is_visible = False
        visible_views = []
        if edi_views:
            visible_views = [
                v['name'] for v in edi_views
                if v.get('visibleFieldIds') is None or f['id'] in v.get('visibleFieldIds')
            ]
            is_visible = len(visible_views) > 0
        entry = {
            settings['table_id_field']: t['id'],
            settings['table_name_field']: t['name'],
            settings['field_id_field']: f['id'],
            settings['field_name_field']: f['name'],
            settings['field_description_field']: f.get('description'),
            settings['field_type_field']: f['type'],
            settings['field_options_field']: get_field_options(f),
            settings['field_camelcase_name_field']: to_camel_case(f['name']),
            settings['edi_field']: 'Yes' if is_visible else 'No',
        }
        for view_name in sorted_edi_views:
            entry[view_name] = 'No'
            if view_name in edi_views_dict:
                v = edi_views_dict[view_name]
                if v.get('visibleFieldIds') is None or f['id'] in v.get('visibleFieldIds'):
                    entry[view_name] = 'Yes'
        metadata_entries.append(entry)

# Create DataFrame
df = pd.DataFrame(metadata_entries)

# Export to Excel file on the specified path
edi_path = Path.home() / 'Documents' / 'Work' / 'Priority' / 'EDI' / 'Airtable EDI Metadata.xlsx'
df.to_excel(edi_path, index=False)

print(f"Exported entire base metadata to {edi_path}")
print("**Done**")