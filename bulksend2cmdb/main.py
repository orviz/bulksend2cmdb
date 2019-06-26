import logging
import requests
import simplejson as json
from six.moves import urllib
import sys
import uuid


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests').setLevel(logging.DEBUG)
logging.getLogger('urllib').setLevel(logging.DEBUG)
logging.getLogger('json').setLevel(logging.DEBUG)


records = []


def get_entity_key(entity):
    '''
    Returns the entity key that contains the entity ID value (according to CMDB schema)

    :entity: entity type (one of provider|service|tenant|image|flavor)
    '''
    return {
        'provider': 'id',
        'service': 'endpoint',
        'tenant': 'tenant_id',
        'image': 'image_id',
        'flavor': 'flavor_id'}[entity]


def get_parent_key(entity):
    '''
    Returns the parent's entity key that contains the entity ID value (according to CMDB schema)

    :entity: entity type (one of provider|service|tenant|image|flavor)
    '''
    return {
        'provider': None,
        'service': 'provider_id',
        'tenant': 'service',
        'image': 'tenant_id',
        'flavor': 'tenant_id'}[entity]


def get_children_entity(entity):
    '''
    Returns the list of entities that are related with the given entity.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    '''
    return {
        'provider': ['service'],
        'service': ['tenant'],
        'tenant': ['image', 'flavor'],
        'image': [],
        'flavor': []}[entity]


def get_from_cip(entity, cip_records, parent=None):
    '''
    Retrieves the records from CIP that match the entity type. If parent is given, it 
    filters CIP records according to the entity's parent value.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :cip_records: CIP data
    :parent: parent's entity CIP id value
    '''
    l = []
    parent_key = get_parent_key(entity)
    for record in cip_records:
        if record['type'] == entity:
            if parent:
                record_parent = record['data'][parent_key]
                if record_parent == parent:
                    l.append(record)
            else:
                l.append(record)
    return l


def get_from_cmdb(entity, cip_id=None, parent=None):
    '''
    Obtains, if exists, a matching CMDB record based on the entity type
    and its CIP id. If parent is given, it filters CMDB records according
    to the entity's parent value.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :cip_id: entity CIP id value to match
    :parent: parent's entity CMDB id value
    '''
    parent_key = get_parent_key(entity)
    with open('CMDB_IFCA.json') as json_file:
        cmdb_data = json.load(json_file)
    # filtering
    filtered_data = []
    for record in cmdb_data:
        if record['type'] == entity:
            if parent:
                if record['data'][parent_key] == parent:
                    filtered_data.append(record)
            else:
                # workaround for provider case
                record['data']['id'] = record['_id']
                filtered_data.append(record)
    # matching
    if cip_id:
        entity_key = get_entity_key(entity)
        for record in filtered_data:
            if cip_id == record['data'][entity_key]:
                return record


def generate_records(entity, cip_data, parent=None, parent_cmdb=None):
    '''
    Recursively generates the records, obtained from CIP, that will be pushed to CMDB.

    The function follows a top-down approach, starting with the first entity
    in the hierarchy (i.e. provider), iterating downwards until the last entity
    has been processed. At each entity level, the function iterates over the entire
    set of input (CIP) records, trying to match them with current CMDB data. If no match
    is found, it will add a new entry in CMDB.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :cip_data: Records received from CIP 
    :parent: parent's entity CIP id value
    :parent_cmdb: parent's entity CMDB id value
    '''
    logging.debug('Recursive call (locals: %s)' % locals())

    cip = get_from_cip(entity,
                       cip_data,
                       parent=parent)
    logging.debug(('Got records from CIP based on entity <%s> and parent '
                   '<%s>: %s' % (entity, parent, cip)))

    entity_children = get_children_entity(entity)
    entity_key = get_entity_key(entity)
    logging.debug('Entity key is <%s>' % entity_key)
    parent_key = get_parent_key(entity)
    logging.debug('Parent key is <%s>' % parent_key)

    for item in cip:
        cip_id_value = item['data'][entity_key]
        cmdb_match = get_from_cmdb(entity,
                                   cip_id=cip_id_value,
                                   parent=parent_cmdb)
        cmdb_id_value = None
        if cmdb_match:
            logging.debug(('Found record in CMDB matching entity <%s> and CIP '
                           'id <%s>' % (entity, cip_id_value)))
            cmdb_id_value = cmdb_match['_id']
            item['_rev'] = cmdb_match['_rev']
        else:
            # generate UUID __only__ when there are children entities
            if entity_children:
                logging.debug(('Generating CMDB id (UUID-based) as entity '
                               '<%s> has children entities' % entity))
                cmdb_id_value = str(uuid.uuid4())
        if cmdb_id_value:
            item['_id'] = cmdb_id_value
        item['data'][parent_key] = parent_cmdb
        records.append(item)

        logging.debug('Resultant record: %s' % json.dumps(item, indent=4))
        for child in entity_children:
            generate_records(child,
                             cip_data,
                             records,
                             parent=cip_id_value,
                             parent_cmdb=cmdb_id_value)


def main():
    cip_data = json.load(sys.stdin)
    generate_records('provider', cip_data)
    print(json.dumps(records, indent=4))
