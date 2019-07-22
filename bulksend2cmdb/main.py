import argparse
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
cip_data = json.load(sys.stdin)
opts = None


def cmdb_get_request(url_endpoint):
    '''
    Performs GET HTTP requests to CMDB

    :url_endpoint: URL endpoint
    '''
    l = []
    url = urllib.parse.urljoin(
        opts.cmdb_read_endpoint, url_endpoint)
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        r_json = r.json()
        if not r_json.has_key('error'):
            # 'provider' has no rows
            if not r_json.has_key('rows'):
                l.append(r_json)
            else:
                for item in r_json['rows']:
                    l.append(item['doc'])
        else:
            logging.debug('Got CMDB error in HTTP request: %s' % r_json)
    return l


def set_bulk_format(json_data):
    '''
    Set JSON data according to CouchDB format for bulk operations.

    :json_data: JSON data
    '''
    d = {}
    d['docs'] = json_data
    return json.dumps(d)


def cmdb_bulk_post(json_data):
    '''
    Performs BULK POST HTTP request to CMDB

    :json_data: JSON data
    '''
    headers = {
        'Content-Type': 'application/json',
    }
    url = urllib.parse.urljoin(
        opts.cmdb_write_endpoint,
        '_bulk_docs')
    url = opts.cmdb_write_endpoint+'/_bulk_docs'
    logging.debug("BULK POSTING TO %s" % url)
    bulk_json_data = set_bulk_format(json_data)
    if opts.oidc_token:
        headers['Authorization'] = 'Bearer %s' % opts.oidc_token
        r = requests.post(url, headers=headers, data=bulk_json_data)
    elif (opts.cmdb_db_user and opts.cmdb_db_pass):
        s = requests.Session()
        s.auth = (opts.cmdb_db_user, opts.cmdb_db_pass)
        r = s.post(url, headers=headers, data=bulk_json_data)
    else:
        logging.error(('No authorization credentials (OpenID token OR '
                       'user/password) were provided'))
    logging.debug('Result/s of BULK POST: %s' % r.content)


def get_entity_key(entity):
    '''
    Returns the entity key that contains the entity ID value (according to
    CMDB schema).

    :entity: entity type (one of provider|service|tenant|image|flavor)
    '''
    return {
        'provider': 'name',
        'service': 'endpoint',
        'tenant': 'tenant_id',
        'image': 'image_id',
        'flavor': 'flavor_id'}[entity]


def get_parent_key(entity):
    '''
    Returns the parent's entity key that contains the entity ID value
    (according to CMDB schema).

    :entity: entity type (one of provider|service|tenant|image|flavor)
    '''
    return {
        'provider': 'name',
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


def get_from_cip(entity, parent=None, data=None):
    '''
    Retrieves the records from CIP that match the entity type. If parent is
    given, it filters CIP records according to the entity's parent value.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :parent: parent's entity CIP id value
    :data: optional data (default: global 'cip_data' variable)
    '''
    l = []
    parent_key = get_parent_key(entity)
    _cip_data = cip_data
    if data:
        _cip_data = data
    for record in _cip_data:
        if record['type'] == entity:
            if parent:
                record_parent = record['data'][parent_key]
                if record_parent == parent:
                    l.append(record)
            else:
                l.append(record)
    return l


def get_from_cmdb_file(entity, parent):
    '''
    Returns entity-based CMDB data stored in a JSON file.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :parent: parent's entity CMDB id value. In the specific case of the
             provider this variable does not point to the parent, but to the
             provider id.
    '''
    with open(opts.cmdb_data_file) as json_file:
        cmdb_data = json.load(json_file)
    # filtering
    parent_key = get_parent_key(entity)
    filtered_data = []
    for record in cmdb_data:
        if record['type'] == entity:
            if record['data'][parent_key] == parent:
                filtered_data.append(record)
    return filtered_data


def get_from_cmdb_http(entity, parent):
    '''
    Get entity-based CMDB data via HTTP

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :parent: parent's entity CMDB id value. In the specific case of the
             provider this variable does not point to the parent, but to the
             provider id.
    '''
    if entity == 'provider':
	url_endpoint = 'provider/id/%s?include_docs=true' % parent
    elif entity == 'service':
        url_endpoint = ('service/filters/provider_id/%s'
                        '?include_docs=true' % parent)
    elif entity == 'tenant':
        url_endpoint = ('tenant/filters/service_id/%s'
                        '?include_docs=true' % parent)
    elif entity == 'image':
	url_endpoint = 'image/filters/tenant_id/%s?include_docs=true' % parent
    elif entity == 'flavor':
	url_endpoint = 'flavor/filters/tenant_id/%s?include_docs=true' % parent

    return cmdb_get_request(url_endpoint)


def get_from_cmdb(entity, cip_id=None, parent=None):
    '''
    Obtains, if exists, a matching CMDB record based on the entity type
    and its CIP id. If parent is given, it filters CMDB records according
    to the entity's parent value.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :cip_id: entity CIP id value to match
    :parent: parent's entity CMDB id value
    '''
    if opts.cmdb_data_file:
        cmdb_data = get_from_cmdb_file(entity, parent)
    elif opts.cmdb_read_endpoint:
        cmdb_data = get_from_cmdb_http(entity, parent)
    else:
        cmdb_data = {}

    # matching
    if cip_id:
        entity_key = get_entity_key(entity)
        for record in cmdb_data:
            if cip_id == record['data'][entity_key]:
                return record
    else:
        return cmdb_data


def generate_records(entity, parent=None, parent_cmdb=None):
    '''
    Recursively generates the records, obtained from CIP, that will be pushed
    to CMDB.

    The function follows a top-down approach, starting with the first entity
    in the hierarchy (i.e. provider), iterating downwards until the last entity
    has been processed. At each entity level, the function iterates over the
    entire set of input (CIP) records, trying to match them with current CMDB
    data. If no match is found, it will add a new entry in CMDB.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :parent: parent's entity CIP id value
    :parent_cmdb: parent's entity CMDB id value
    '''
    logging.debug('Recursive call (locals: %s)' % locals())

    cip = get_from_cip(entity,
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
        # special 'provider' case
        if entity == 'provider' and not parent_cmdb:
            parent_cmdb = cip_id_value
        cmdb_match = get_from_cmdb(entity,
                                   cip_id=cip_id_value,
                                   parent=parent_cmdb)
        cmdb_id_value = None
        if cmdb_match:
            logging.debug(('Found record in CMDB matching entity <%s> and CIP '
                'id <%s> [action: update]' % (entity, cip_id_value)))
            cmdb_id_value = cmdb_match['_id']
            item['_rev'] = cmdb_match['_rev']
        else:
            logging.debug('Record not in CMDB [action: create]')
            # generate UUID __only__ when there are children entities
            if entity_children:
                # special 'provider' case -> _id == sitename
                if entity == 'provider':
                    logging.debug(('Generating provider CMDB id as the site '
                                   'name value'))
                    cmdb_id_value = parent_cmdb
		else:
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
                             parent=cip_id_value,
                             parent_cmdb=cmdb_id_value)


def generate_deleted_records(entity, parent=None):
    '''
    Iterate over CMDB records, which are related (parent-child relations) to
    the already generated ones (global records), to find the ones that are not
    present in the latter.

    Note that broken CMDB records (e.g. no existing parent) are not detected,
    and thus they won't be removed.

    :entity: entity type (one of provider|service|tenant|image|flavor)
    :parent: parent's entity id value (same for both global records and CMBD)
    '''
    logging.debug('Recursive call (locals: %s)' % locals())

    cmdb = get_from_cmdb(entity,
                         parent=parent)
    logging.debug('CMDB data for entity <%s>: %s' % (entity, cmdb))

    entity_children = get_children_entity(entity)
    entity_key = get_entity_key(entity)
    records_entity_data = [item['data'][entity_key] for item in get_from_cip(
        entity, parent=parent, data=records)]

    for cmdb_item in cmdb:
        if cmdb_item['data'][entity_key] not in records_entity_data:
            logging.debug(('Record from CMDB not found in CIP data '
                           '(parent: %s): %s [action: delete]' % (
                               parent,
                               cmdb_item)))
            cmdb_item['_deleted'] = True
            records.append(cmdb_item)
        for child in entity_children:
            generate_deleted_records(child,
                                     parent=cmdb_item['_id'])


class ServiceUtils(object):
    @staticmethod
    def get_id_from_cmdb(endpoint, provider_id):
        '''
        Gets CMDB id of the matching service endpoint
        '''
        r = get_from_cmdb('service', parent=provider_id)
        for record in r:
            if endpoint == record['data']['endpoint']:
                return record['_id']


def generate_additional_customization():
    '''
    Iterate over the generate list of records to add modifications.
    '''
    for record in records:
        #print("RECORD: %s" % record)
        if record['type'] == 'service':
            # Manage 'service_parent_id' for children services
            if 'service_parent_id' in record['data'].keys():
                logging.debug(('Found service_parent_id for record '
                               '<%s>' % record))
                service_parent_id = ServiceUtils.get_id_from_cmdb(
                    record['data']['service_parent_id'],
                    record['data']['provider_id'])
                if service_parent_id:
                    logging.info(('Customizing service_parent_id <%s> with '
                                  'CMDB service ID: %s' % (
                                      record,
                                      service_parent_id)))
                    record['data']['service_parent_id'] = service_parent_id


def get_input_opts():
    '''
    Manage input arguments.
    '''
    parser = argparse.ArgumentParser(description=('CIP->CMDBv1 data pusher.'))
    parser.add_argument('--cmdb-read-endpoint',
                        metavar='URL',
                        help='Specify CMDB read URL')
    parser.add_argument('--cmdb-write-endpoint',
                        metavar='URL',
                        help='Specify CMDB write URL')
    parser.add_argument('--oidc-token',
                        metavar='TOKEN',
                        help='OpenID (bearer) token value for authentication')
    parser.add_argument('--cmdb-db-user',
                        metavar='USERNAME',
                        help=('With password authentication, this specifies '
                              'CMDB username'))
    parser.add_argument('--cmdb-db-pass',
                        metavar='PASSWORD',
                        help=('With password authentication, this specifies '
                              'CMDB password'))
    parser.add_argument('--cmdb-data-file',
                        metavar='JSON_FILE',
                        help=('Specify a JSON file for CMDB data rather than '
                              'getting remotely'))
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Do not post to remote CMDB service')
    return parser.parse_args()


def main():
    global opts
    opts = get_input_opts()

    # generate all records
    generate_records('provider')
    # delete __only__ starting from tenants
    services = get_from_cip('service', data=records)
    for service in services:
        generate_deleted_records('tenant', parent=service['_id'])
    # additional customization
    generate_additional_customization()
    logging.debug(json.dumps(records, indent=4))

    # bulk post
    if not opts.dry_run:
        cmdb_bulk_post(records)
