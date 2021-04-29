#!/usr/bin/env python3
#
# 
# 
# 
# Kim Brugger (14 Sep 2018), contact: kim@brugger.dk

import sys
import re
import pprint
from ecc.utils import make_node_name

pp = pprint.PrettyPrinter(indent=4)
import random

from munch import Munch
import kbr.log_utils as logger

import ecc.openstack_class as openstack_class
import ecc.slurm_utils as slurm_utils
import ecc.utils as ecc_utils
import ecc.ansible_utils as ansible_utils
import ecc.cloudflare_utils as cloudflare_utils

# Not sure if this is still needed.
import logging
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('keystoneauth').setLevel(logging.CRITICAL)
logging.getLogger('stevedore').setLevel(logging.CRITICAL)
logging.getLogger('concurrent').setLevel(logging.CRITICAL)
logging.getLogger('openstack').setLevel(logging.CRITICAL)
logging.getLogger('dogpile').setLevel(logging.CRITICAL)

config = None
openstack = None
nodes = {}

def set_config(new_config:dict):
    global config
    config = new_config


def openstack_connect(config):
    global openstack
    openstack = openstack_class.Openstack()
    openstack.connect(**config)

def servers(filter:str=None):
    servers = openstack.servers()

    if filter:
        filter = re.compile(filter)
        tmp_list = []
        for server in servers:
 #           print("--", server['name'], filter.match(server['name']))
            if re.search(filter, server['name']):
                tmp_list.append(server)

        servers = tmp_list

    return servers


def update_nodes_status():
    vnodes = servers(config.ecc.name_regex)
    snodes = slurm_utils.nodes()

    global nodes
    nodes = {}

    for vnode in vnodes:
        if vnode['name'] not in nodes:
            nodes[vnode['name']] = {}
            nodes[vnode['name']]['vm_id'] = vnode['id']
            nodes[vnode['name']]['name'] = vnode['name']
            nodes[vnode['name']]['ip'] = vnode.get('ip', [])
            nodes[vnode['name']]['vm_state'] = vnode['status']
            nodes[vnode['name']]['slurm_state'] = 'na'
            nodes[vnode['name']]['timestamp'] = ecc_utils.timestamp()

        elif 'vm_state' not in nodes[vnode['name']] or nodes[vnode['name']]['vm_state'] != vnode['status']:
            nodes[vnode['name']]['vstate'] = vnode['status']
            nodes[vnode['name']]['timestamp'] = ecc_utils.timestamp()


    for snode in snodes:
        if snode['name'] not in nodes:
            nodes[snode['name']] = {}
            nodes[snode['name']]['vm_id'] = None
            nodes[snode['name']]['name'] = snode['name']
            nodes[snode['name']]['ip'] = []
            nodes[snode['name']]['vm_state'] = None
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()

        elif 'slurm_state' not in nodes[snode['name']]or nodes[snode['name']]['slurm_state'] != snode['state']:
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()


#    pp.pprint(nodes)


def nodes_info(update:bool=True):
    if update:
        update_nodes_status()

    global nodes

    return nodes


def nodes_idle(update:bool=False):

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) == 'idle' and node.get('vm_state', None) == 'active':
            count += 1

    return count


def nodes_total(update:bool=False):

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) in ['mix', 'idle', 'alloc'] and node.get('vm_state', None) == 'active':
            count += 1

    return count


def delete_idle_nodes(count:int=1):
    """ Delete idle nodes, by default one node is vm_deleted
    """


    nodes = nodes_info().values()
    nodes_to_cull = []
    for n in nodes:
        if n['slurm_state'] == 'idle' and n['vm_id'] is not None:
            nodes_to_cull.append(n['vm_id'])

    delete_nodes( nodes_to_cull[0:count] )
    return


def delete_node(ids:str):
    # wrapper for the function below
    return delete_nodes( ids )


def delete_nodes(ids:[]):

    if not isinstance( ids, list):
        ids = [ids]

    for id in ids:
        if id is None:
            continue 
        logger.info("deleting node {}".format( id ))
        vm = openstack.server( id )

        logger.info('deleting DNS entry...')
        cloudflare_utils.purge_name( vm['name'])
        logger.info('deleting VM...')
        openstack.server_delete( id )

    logger.info('running playbook')
    ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)

    return




def create_nodes(cloud_init_file:str=None, count=1):


#    resources = openstack.get_resources_available()
    global nodes

    try:
        for _ in range(0, count):
            node_id = next_id(names=openstack.server_names())
            node_name = config.ecc.name_template.format( node_id)
            print(f"creating node with name {node_name}")

            node_id = openstack.server_create( name=node_name,
                                           userdata_file=cloud_init_file,
                                           **config.ecc )

            logger.debug("Execute server {}/{} is vm_booting".format( node_id, node_name))
            # This is a blocking call, so will hang here till the server is online.
            openstack.wait_for_log_entry(node_id)
            node_ips = openstack.server_ip(node_id)
            try:
                cloudflare_utils.add_record('A', node_name, node_ips[0], 1000)
            except:
                print(f"failed to add dns entry: 'add_record('A', {node_name}, {node_ips[0]}, 1000)'")

            nodes[node_name] = {}
            nodes[node_name]['vm_id'] = node_id
            nodes[node_name]['vm_state'] = 'booting'
            nodes[node_name] = node_ips


    except Exception as e:
        logger.warning("Could not create execute server")
        logger.debug("Error: {}".format(e))
        return

    try:
        ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)
    except:
        print(f"failed to run playbook: 'run_playbook({config.ecc.ansible_cmd}, host={node_ips[0]}, cwd={config.ecc.ansible_dir})'")
        return

    return node_name


def next_id(names, regex:str=None) -> int:

    print(f"Node names {names}")

    if regex is None:
        regex = config.ecc.name_regex
    regex = re.compile(regex)

    ids = []
    for name in names:
        g = re.match( regex, name)
        if g:
            ids.append( int(g.group(1)))

#    print( ids )

    if ids == []:
        return 1

    ids = sorted(ids)

    if ids[0] > 1:
        return ids[0] - 1


    for i in range(0, len(ids) - 1):
        if ids[ i ] + 1 < ids[ i + 1]:
            return ids[ i ] + 1

    return ids[ -1 ] + 1

def write_config_file(filename:str='ecc.yml') -> None:
    if os.path.isfile( args.create_config ):
        raise RuntimeError('Config file already exists, please rename before creating a new one')

    config = '''
openstack:
    auth_url: <AUTH URL>
    password: <PASSWORD>
    project_domain_name: <PROJECT DOMAIN>
    project_name: <PROJECT NAME>
    region_name: <REGION>
    user_domain_name: <USER DOMAIN>
    username: <USER EMAIL>

ecc:
    log: ecc.log
    nodes_max: 6
    nodes_min: 1
    nodes_spare: 1
    sleep: 30

    flavor: m1.medium
    image: GOLD CentOS 7
    key: <SSHKEY>
    network: dualStack
    security_groups: slurm-node
    name_template: "ecc{}.usegalaxy.no"
    cloud_init: <PATH>/ecc_node.yaml
    ansible_dir: <PATH, eg: /usr/local/ansible/infrastructure-playbook/env/test>
    ansible_cmd: "<CMD, EG: ./venv/bin/ansible-playbook -i ecc_nodes.py slurm.yml"

    cloudflare_apikey: <API KEY>
    cloudflare_email: <EMAIL>'''


    with open(filename, 'w') as outfile:
        outfile.write(config)
        outfile.close()

    return None

