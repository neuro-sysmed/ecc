#!/usr/bin/env python3
#
# 
# 
# 
# Kim Brugger (14 Sep 2018), contact: kim@brugger.dk

import sys
import os
import re
import pprint

pp = pprint.PrettyPrinter(indent=4)
import random

from munch import Munch
import kbr.log_utils as logger

import ecc.openstack_class as openstack_class
import ecc.azure_class as azure_class
import ecc.slurm_utils as slurm_utils
import ecc.utils as ecc_utils
import ecc.ansible_utils as ansible_utils
import ecc.cloudflare_utils as cloudflare_utils
from ecc.utils import make_node_name

# Not sure if this is still needed.
import logging
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('keystoneauth').setLevel(logging.CRITICAL)
logging.getLogger('stevedore').setLevel(logging.CRITICAL)
logging.getLogger('concurrent').setLevel(logging.CRITICAL)
logging.getLogger('openstack').setLevel(logging.CRITICAL)
logging.getLogger('dogpile').setLevel(logging.CRITICAL)

config = None
cloud = None
nodes = {}

def set_config(new_config:dict) -> None:
    global config
    config = new_config


def openstack_connect(config):
    global cloud
    cloud = openstack_class.Openstack()
    cloud.connect(**config)

def azure_connect(config):
    global cloud
    cloud = azure_class.Azure()
    cloud.connect(config.subscription_id)


def servers(filter:str=None):
    servers = cloud.servers()

    if filter:
        filter = re.compile(filter)
        tmp_list = []
        for server in servers:
 #           print("--", server['name'], filter.match(server['name']))
            if re.search(filter, server['name']):
                tmp_list.append(server)

        servers = tmp_list

    return servers


def update_nodes_status() -> None:
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


def nodes_info(update:bool=True) -> list:
    if update:
        update_nodes_status()

    global nodes

    return nodes


def nodes_idle(update:bool=False) -> int:

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) == 'idle' and node.get('vm_state', None) in ['active', 'running']:
            count += 1

    return count

def nodes_idle_timelimit(update:bool=False, limit:int=300) -> list:

    if update:
        update_nodes_status()

    idle_nodes = []
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) == 'idle' and node.get('vm_state', None) == 'active':
            idle_time = node.timestamp - ecc_utils.timestamp()
            if idle_time >= limit:
                idle_nodes.append(n['vm_id'])

    return idle_nodes


def nodes_total(update:bool=False) -> int:

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) in ['mix', 'idle', 'alloc'] and node.get('vm_state', None) in ['active', 'running']:
            count += 1

    return count


def delete_idle_nodes(count:int=1) -> None:
    """ Delete idle nodes, by default one node is vm_deleted
    """

    nodes = nodes_info().values()
    nodes_to_cull = []
    for n in nodes:
        if n['slurm_state'] == 'idle' and n['vm_id'] is not None:
            nodes_to_cull.append(n['vm_id'])

    delete_nodes( nodes_to_cull[0:count] )
    return


def delete_node(ids:str) -> None:
    # wrapper for the function below
    return delete_nodes( ids )


def delete_nodes(ids:list=[], count:int=None) -> None:

    if count is not None:
        idle_nodes = nodes_idle()
        for idle_node in idle_nodes:
            ids.append( idle_node['vm_id'])
            count -= 1
            if count <= 0:
                break

    if not isinstance( ids, list):
        ids = [ids]

    for id in ids:
        if id is None:
            continue 
        
        if id in nodes:
            id = nodes[id]['vm_id']

        logger.info("deleting node {}".format( id ))
        vm = cloud.server( id )

        if 'cloudflare' in config.ecc:
            logger.info('deleting DNS entry...')
            cloudflare_utils.purge_name( vm['name'])

        logger.info('deleting VM...')
        cloud.server_delete( id )

    if 'ansible_cmd' in config.ecc:
        logger.info('running playbook')
        ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)

    return




def create_nodes(cloud_init_file:str=None, count:int=1, hostnames:list=[]):


#    resources = openstack.get_resources_available()
    global nodes
    created_nodes = []

    try:
        for _ in range(0, count):
            if len(hostnames):
                node_name = hostnames.pop(0)
            else:
                node_id = next_id(names=cloud.server_names())
                node_name = config.ecc.name_template.format( node_id)

            print(f"creating node with name {node_name}")

            node_id = cloud.server_create( name=node_name,
                                           userdata_file=cloud_init_file,
                                           **config.ecc )


            if 'cloudflare' in config.ecc:
                try:
                    cloudflare_utils.add_record('A', node_name, node_ips[0], 1000)
                except:
                    print(f"failed to add dns entry: 'add_record('A', {node_name}, {node_ips[0]}, 1000)'")


            node_ips = cloud.server_ip(node_id)

            nodes[node_name] = {}
            nodes[node_name]['vm_id'] = node_id
            nodes[node_name]['vm_state'] = 'booting'
            nodes[node_name] = node_ips
            
            create_nodes.append(node_name)


    except Exception as e:
        logger.warning("Could not create execute server")
        logger.debug("Error: {}".format(e))
        return

    if 'ansible_cmd' in config.ecc:
        try:
            ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)
        except:
            print(f"failed to run playbook: 'run_playbook({config.ecc.ansible_cmd}, host={node_ips[0]}, cwd={config.ecc.ansible_dir})'")
            return
    
    for n in create_nodes:
        slurm_utils.update_node_state( n, "resume")


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

def write_config_file(filename:str='ecc.yaml') -> None:
    if os.path.isfile( filename ):
        raise RuntimeError('Config file already exists, please rename before creating a new one')

    config = '''
openstack:
    auth_url: https://api.uh-iaas.no:5000/v3
    password: <PASSWORD>
    project_domain_name: dataporten
    project_name: elixir-nrec-prod-backend
    region_name: bgo
    user_domain_name: dataporten
    username: <USERNAME>

azure:
    subscription_id: <SUBSCRIPTION ID>    

ecc:
    log: ecc.log
    nodes_max: 6
    nodes_min: 1
    nodes_spare: 1
    sleep: 30
    name_template: "ecc{}.usegalaxy.no"

    image: GOLD CentOS 7
    cloud_init: <PATH>/ecc_node.yaml
    ansible_dir: <PATH, eg: /usr/local/ansible/infrastructure-playbook/env/test>
    ansible_cmd: "<CMD, EG: ./venv/bin/ansible-playbook -i ecc_nodes.py slurm.yml"

    # If doing DNS for nodes
    cloudflare_apikey: <API KEY>
    cloudflare_email: <EMAIL>


    # openstack variables
    flavor: m1.medium
    key: <SSHKEY>
    network: dualStack
    security_groups: slurm-node

    
    #azure variables
    compute_group: FOR-NEURO-SYSMED-UTV-COMPUTE
    network_group: FOR-NEURO-SYSMED-UTV-NETWORK
    virtual_network: FOR-NEURO-SYSMED-UTV-VNET
    virtual_subnet:  WorkloadsSubnet
    vm_size: Standard_D2_v2
    admin_username: root
    admin_password: <SECRET!>
    
    '''

    #

    with open(filename, 'w') as outfile:
        outfile.write(config)
        outfile.close()

    return None

