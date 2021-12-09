import json
import sys
import glob
import gzip

ENCODE_PROCESSING_PIPELINE_UUID = 'a558111b-4c50-4b2e-9de8-73fd8fd3a67d'
RAW_OUTPUT_TYPES = ['reads', 'rejected reads', 'raw data', 'reporter code counts', 'intensity values', 'idat red channel', 'idat green channel']

def file_is_md5sum_constrained(properties):
    conditions = [
        properties.get('lab') != ENCODE_PROCESSING_PIPELINE_UUID,
        properties.get('output_type') in RAW_OUTPUT_TYPES
    ]
    return any(conditions)

class CustomKeys:
    def Publication(self, keys, properties):
        keys.setdefault('alias', []).extend(keys.get("Publication:identifier", []))

    def Image(self, keys, properties):
        value = properties['attachment']['download']
        keys.setdefault('Image:filename', []).append(value)

    def Donor(self, keys, properties):
        if properties.get('status') != 'replaced':
            if 'external_ids' in properties:
                keys.setdefault('alias', []).extend(properties['external_ids'])

    def Page(self, keys, properties):
        parent = properties.get('parent')
        name = properties['name']
        value = name if parent is None else '{}:{}'.format(parent, name)
        keys.setdefault('Page:location', []).append(value)

    def Replicate(self, keys, properties):
        if '_fake' in properties:
            return
        value = '{experiment}/{biological_replicate_number}/{technical_replicate_number}'.format(
            **properties)
        keys.setdefault('Replicate:experiment_biological_technical', []).append(value)

    def AntibodyLot(self, keys, properties):
        if '_fake' in properties:
            return
        source = properties['source']
        product_id = properties['product_id']
        lot_ids = [properties['lot_id']] + properties.get('lot_id_alias', [])
        values = ('{}/{}/{}'.format(source, product_id, lot_id) for lot_id in lot_ids)
        keys.setdefault('AntibodyLot:source_product_lot', []).extend(values)

    def File(self, keys, properties):
        if properties.get('status') != 'replaced':
            if 'md5sum' in properties and file_is_md5sum_constrained(properties):
                value = 'md5:{md5sum}'.format(**properties)
                keys.setdefault('alias', []).append(value)
            # Ensure no files have multiple reverse paired_with
            if 'paired_with' in properties:
                keys.setdefault('File:paired_with', []).append(properties['paired_with'])
            if 'external_accession' in properties:
                keys.setdefault('external_accession', []).append(
                    properties['external_accession'])


def extract_schema_links(schema):
    if not schema:
        return
    for key, prop in schema['properties'].items():
        if 'items' in prop:
            prop = prop['items']
        if 'properties' in prop:
            for path in extract_schema_links(prop):
                yield (key,) + path
        elif 'linkTo' in prop:
            yield (key,)


def simple_path_ids(obj, path):
    if isinstance(path, str):
        path = path.split('.')
    if not path:
        yield obj
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not isinstance(value, list):
        value = [value]
    for member in value:
        for result in simple_path_ids(member, remaining):
            yield result



class TypeInfo:
    def __init__(self, typename, schema):
        self.typename = typename
        self.schema = schema
        self.remove = set()
        self.touuid = set()
        self.nosubmit = set()
        self.uniqueKeys = {}
        #self.schema_links = sorted('.'.join(path) for path in extract_schema_links(schema))
        self.walk(schema)
        for name, subschema in schema.get('properties',{}).items():
            if subschema["type"] == "array":
                subschema = subschema["items"]
            uniqueKey = subschema.get('uniqueKey')
            if uniqueKey is not None:
                if uniqueKey is True:
                    self.uniqueKeys[name] = f'{self.typename}:{name}'
                elif ':' in uniqueKey:
                    self.uniqueKeys[name] = uniqueKey.capitalize()
                else:
                    self.uniqueKeys[name] = uniqueKey            

    def walk(self, schema, path=()):
        for name, subschema in schema.get('properties',{}).items():
            subpath = path + (name,)
            if subschema.get("notSubmittable"):
                self.nosubmit.add(subpath)
            if subschema["type"] == "array":
                subschema = subschema["items"]
            if 'linkFrom' in subschema:
                self.remove.add(subpath)
            elif 'linkTo' in subschema:
                self.touuid.add(subpath)
            self.walk(subschema, subpath)

    def calculated(self, obj, id_uuid, path=()):
        if isinstance(obj, str):
            if path in self.touuid and obj[0] == '/':
                if obj in id_uuid:
                    return id_uuid[obj]
                else:
                    print(obj, file=sys.stderr)
        elif isinstance(obj, list):
            return [self.calculated(v, id_uuid, path) for v in obj]
        elif isinstance(obj, dict):
            result = {}
            for name, value in obj.items():
                subpath = path + (name,)
                if subpath not in self.remove:
                    result[name] = self.calculated(value, id_uuid, subpath)
            return result
        return obj

    def raw(self, obj, path=()):
        if isinstance(obj, list):
            return [self.raw(v, path) for v in obj]
        elif isinstance(obj, dict):
            result = {}
            for name, value in obj.items():
                subpath = path + (name,)
                if subpath not in self.nosubmit:
                    result[name] = self.raw(value, subpath)
            return result
        return obj

    def links(self, properties):
        return {
            '.'.join(path): sorted(set(simple_path_ids(properties, path)))
            for path in self.touuid
        }

    def unique(self, properties):
        keys = {}
        for propname, keyname in self.uniqueKeys.items():
            if propname in properties:
                value = properties[propname]
                keys.setdefault(keyname, []).extend(value if isinstance(value, list) else [value])
        if 'accession' in self.schema['properties']:
            keys.setdefault('accession', []).extend(properties.get('alternate_accessions', []))
            if properties.get('status') != 'replaced' and 'accession' in properties:
                keys['accession'].append(properties['accession'])
        custom = getattr(CustomKeys, self.typename, None)
        if custom:
            custom(self, keys, properties)
        return { k: sorted(set(v)) for k, v in keys.items() if v }


def main():
    profiles = json.load(open("profiles.json"))
    profiles['AnalysisStepVersion']['properties']['name']['uniqueKey'] = True
    profiles['AnalysisStep']['properties']['name']['uniqueKey'] = True
    profiles['BiosampleType']['properties']['name']['uniqueKey'] = True
    profiles['Target']['properties']['name']['uniqueKey'] = True
    # profiles['QualityStandard']['properties']['name']['uniqueKey'] = True
    for profile in profiles.values():
        if not isinstance(profile, dict):
            continue
        for name in ['files', 'contributing_files', 'revoked_files']:
            subschema = profile.get('properties', {}).get(name)
            if subschema:
                subschema['items'] = { "type": "string", "linkFrom": "File.dataset" }

    type_infos = {}
    for typename, schema in profiles.items():
        if typename[0] not in ['_', '@']:
            changes = TypeInfo(typename, schema)
            type_infos[typename] = changes
    #id_uuid = {}
    #for filename in ['items.jsonlines', 'fakemissing.jsonlines']:
    #    for line in open(filename):
    #        obj = json.loads(line)
    #        id_uuid[obj['@id']] = obj['uuid']
    #json.dump(id_uuid, open('id_uuid.json', 'w'), separators=(',', ':'))
    #id_uuid = json.load(open('id_uuid.json'))
    id_uuid = {}
    for line in open('id_uuid.tsv'):
        id, uuid = line.strip().split('\t')
        id_uuid[id] = uuid

#    for filename in ['items.jsonlines', 'fakemissing.jsonlines']:
#        for line in open(filename):
#    for filename in glob.glob('*.jsonlines.gz'):
    for filename in [sys.argv[1]]:
        for line in gzip.open(filename):
            doc = json.loads(line)
            obj = doc["object"]
            audits = []
            if 'audit' in obj:
                for arr in obj['audit'].values():
                    audits.extend(a for a in arr if a['path'] == obj['@id'])
                del obj['audit']
            info = type_infos[obj['@type'][0]]
            properties = info.calculated(obj, id_uuid)
            properties['__typename'] = info.typename
            raw = info.raw(properties)
            #links = info.links(raw)
            #unique = info.unique(properties)
            links = doc['links']
            unique = doc["unique_keys"]
            allowed = doc["principals_allowed"]
            print(
                properties['uuid'],
                json.dumps(properties, separators=(',', ':'), sort_keys=True),
                #json.dumps(raw, separators=(',', ':'), sort_keys=True),
                json.dumps(allowed, separators=(',', ':'), sort_keys=True),
                json.dumps(unique, separators=(',', ':'), sort_keys=True),
                json.dumps(links, separators=(',', ':'), sort_keys=True),
                json.dumps(audits, separators=(',', ':'), sort_keys=True),
                sep='\t')


if __name__ == "__main__":
    main()
