#!/usr/bin/env python3
#
# 
# 
# 
# Kim Brugger (14 Sep 2018), contact: kim@brugger.dk

import sys
import time
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


def servers(filter:str=None) -> list:
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
    if 'name_regex' in config.ecc:
        vnodes = servers(config.ecc.name_regex)
    else:
        vnodes = []
        for queue in config.queues:
            vnodes += servers( config.queues[queue].name_regex )

    snodes = slurm_utils.nodes()

    global nodes
    nodes_copy = nodes.copy()


    for vnode in vnodes:

        if vnode['name'] not in nodes:
            nodes[ vnode['name'] ] = {}
            nodes[ vnode['name'] ]['vm_id'] = vnode['id']
            nodes[ vnode['name'] ]['name'] = vnode['name']
            nodes[ vnode['name'] ]['ip'] = vnode.get('ip', [])
            nodes[ vnode['name'] ]['vm_state'] = vnode['status']
            nodes[ vnode['name'] ]['slurm_state'] = 'na'
            nodes[ vnode['name'] ]['partition'] = 'na'
            nodes[ vnode['name'] ]['timestamp'] = ecc_utils.timestamp()

        elif 'vm_state' not in nodes[vnode['name']] or nodes[vnode['name']]['vm_state'] != vnode['status']:
            nodes[ vnode['name'] ]['vm_state'] = vnode['status']
            nodes[ vnode['name'] ]['timestamp'] = ecc_utils.timestamp()

        if vnode['name'] in nodes_copy:
            del nodes_copy[ vnode['name'] ]

    for snode in snodes:
        if snode['name'] not in nodes:
            nodes[snode['name']] = {}
            nodes[snode['name']]['vm_id'] = None
            nodes[snode['name']]['name'] = snode['name']
            nodes[snode['name']]['ip'] = []
            nodes[snode['name']]['vm_state'] = None
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['partition'] = snode['partition']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()

        elif 'slurm_state' not in nodes[snode['name']] or nodes[snode['name']]['slurm_state'] != snode['state']:
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['partition'] = snode['partition']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()



    for node_name in nodes_copy:
        logger.debug( f"{node_name} no longer in list, removing it")
        del nodes[ node_name ]







def nodes_info(update:bool=True) -> list:
    if update:
        update_nodes_status()

    global nodes

    return nodes

def unregistered_nodes(partition:str=None) -> list:
    unregistered = []
    for node_name in nodes:
        node = nodes[ node_name ]
        if node.get('partition', None) == partition and ('slurm_state' not in node or node['slurm_state'] == 'na'):
            unregistered.append( node_name)

    return unregistered

def nodes_idle(update:bool=False) -> int:

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) == 'idle' and node.get('vm_state', None) in ['active', 'running']:
            count += 1

    return count

def nodes_idle_timelimit(update:bool=False, limit:int=300, partition:str=None) -> list:

    if update:
        update_nodes_status()

    idle_nodes = []
    for node in nodes:
        node = nodes[ node ]
        if (node.get('slurm_state', None) == 'idle' and 
           node.get('vm_state', None) in ['active', 'running'] and 
           node.get('partition', None) == partition):
            idle_time = ecc_utils.timestamp() - node['timestamp'] 
            if idle_time >= limit:
                idle_nodes.append(node['vm_id'])

    return idle_nodes

def nodes_total(update:bool=False, partition:str=None):

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if (node.get('slurm_state', None) in ['mix', 'idle', 'alloc', 'down'] and 
            node.get('vm_state', None) in ['active', 'running'] and 
            node.get('partition', None) == partition):

            count += 1

    return count


def slurm_idle_drained_nodes(partition:str=None):
    """ Set node in resume state if is """

    revived = 0

    for node_name in nodes:
        node = nodes[ node_name ]        
        if (node.get('slurm_state', None) in ['drain', 'dead', 'down'] and 
            node.get('vm_state', None) in ['active', 'running'] and 
            node.get('partition', None) == partition ):
            logger.info(f"reviving {node_name}, current state: {node.get('slurm_state', 'NA')}")
            slurm_utils.set_node_resume(node_name)
            revived += 1

    if revived:
        time.sleep( 5 )
        update_nodes_status()



def delete_idle_nodes(count:int=1, nodes_to_cull:list=None) -> None:
    """ Delete idle nodes, by default one node is vm_deleted
    """

    if nodes_to_cull is not None:
        nodes = nodes_info().values()
        nodes_to_cull = []
        for n in nodes:
            if n.get('slurm_state', 'idle') == 'idle' and n['vm_id'] is not None:
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
            logger.debug(f'deleting: changing id from {id} to {nodes[id]["vm_id"]}')
            id = nodes[id]['vm_id']

        try:
            logger.info("deleting node {}".format( re.sub(r'.*/','', id )))
            vm = cloud.server( id )
        except:
            continue

        if 'cloudflare' in config.ecc:
            logger.info('deleting DNS entry...')
            cloudflare_utils.purge_name( vm['name'])

        logger.info('deleting VM...')
        cloud.server_delete( id )
        del nodes[vm.name]

    if 'ansible_cmd' in config.ecc:
        logger.info('Running playbook')
        ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)

    return




def create_nodes(cloud_init_file:str=None, count:int=1, hostnames:list=[], name_regex:str=None, name_template:str=None, vm_size:str=None):


#    resources = openstack.get_resources_available()
    global nodes
    created_nodes = []
    created_ips   = []

    lconfig = config.ecc.copy()

    if name_template is not None:
        lconfig.name_template = name_template

    if name_regex is not None:
        lconfig.name_regex = name_regex

    if vm_size is not None:
        lconfig.vm_size = vm_size

    try:
        for _ in range(0, count):
            if len(hostnames):
                node_name = hostnames.pop(0)
            else:
                node_id = next_id(names=cloud.server_names(), regex=name_regex)
                node_name = lconfig.name_template.format( node_id )    

            logger.info(f"creating node with name {node_name}")

            node_id = cloud.server_create( name=node_name,
                                           userdata_file=cloud_init_file,
                                           **lconfig )


            if 'cloudflare' in config.ecc:
                try:
                    cloudflare_utils.add_record('A', node_name, node_ips[0], 1000)
                except:
                    logger.error(f"failed to add dns entry: 'add_record('A', {node_name}, {node_ips[0]}, 1000)'")


            node_ips = cloud.server_ip(node_id)

            nodes[node_name] = {}
            nodes[node_name]['vm_id'] = node_id
            nodes[node_name]['vm_state'] = 'booting'
            nodes[node_name]['ip'] = node_ips
            
            created_nodes.append(node_name)
            created_ips.append( node_ips[0])


    except Exception as e:
        logger.critical("Could not create VM server")
        logger.critical("Error: {}".format(e))
        # for the dev as this is where we crash
        sys.exit()
        return

    for index, name in enumerate(created_nodes):
        online = ecc_utils.check_host_port(name, 22, duration=180, delay=20, ip=created_ips[ index ] )
        if not online:
            logger.warn(f"{name} is not online yet")
            return None
        else:
            logger.info(f"{name} is online")


    if 'ansible_cmd' in config.ecc:
        logger.info('Running playbook')
        ansible_utils.run_playbook(config.ecc.ansible_cmd, cwd=config.ecc.ansible_dir)
    
    for n in created_nodes:
        slurm_utils.update_node_state( n, "resume")


    return node_name


def next_id(names, regex:str=None) -> int:

#    print(f"Node names {names}")

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

