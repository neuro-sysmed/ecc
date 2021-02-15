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

import ecc.openstack_class as openstack_class
import ecc.slurm_utils as slurm_utils
import ecc.utils as ecc_utils

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
    vnodes = servers(config.ecc.nodes.name_regex)
    snodes = slurm_utils.nodes()

    global nodes
    for vnode in vnodes:
        if vnode['name'] not in nodes:
            nodes[vnode['name']] = {}
            nodes[vnode['name']]['vm_id'] = vnode['id']
            nodes[vnode['name']]['vm_state'] = vnode['status']
            nodes[vnode['name']]['timestamp'] = ecc_utils.timestamp()

        elif 'vm_state' not in nodes[vnode['name']] or nodes[vnode['name']]['vm_state'] != vnode['status']:
            nodes[vnode['name']]['vstate'] = vnode['status']
            nodes[vnode['name']]['timestamp'] = ecc_utils.timestamp()


    for snode in snodes:
        if snode['name'] not in nodes:
            nodes[snode['name']] = {}
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()

        elif 'slurm_state' not in nodes[snode['name']]or nodes[snode['name']]['slurm_state'] != snode['state']:
            nodes[snode['name']]['slurm_state'] = snode['state']
            nodes[snode['name']]['timestamp'] = ecc_utils.timestamp()


    pp.pprint(nodes)


def nodes_idle(update:bool=False):

    if update:
        update_nodes_status()

    count = 0
    for node in nodes:
        node = nodes[ node ]
        if node.get('slurm_state', None) in ['mix', 'idle'] and node.get('vm_state', None) == 'active':
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


def delete_idle_nodes(instances, nodes, nr:int=1, max_heard_from_time:int=300):
    """ Delete idle nodes, by default one node is vm_deleted

    Args:
      nr: nr of nodes to delete (if possible)
      max_heard_from_time: if we have not heard from a node this long, we expect it is dead

    Returns:
      None

    Raises:
      None
    """

    # Subtract the ones that are currently vm_stopping
    nr -= len( instances.get_nodes( vm_state=['vm_stopping']))

    # We have already tagged the number of nodes to be vm_deleted so be
    # conservative and see if we still need to do this later on
    if ( nr <= 0 ):
        logger.debug( 'A suitable amount of nodes are already being killed')
        return

    # loop through the nodes that are deletable
    for node_name in nodes:

        node = instances.find( name = node_name )
        if node is None:
            continue

#        print( node )
        if ( node[ 'node_state' ] == 'node_idle' and node['vm_state'] in ['vm_active', 'vm_booting']):
            logger.debug("Killing node {}".format( node_name ))


            cloud = instances.get_cloud( node['cloud'])

            volumes = cloud.volumes_attached_to_server(node['id'])
            cloud.detach_volumes_from_server(node['id'])

            for volume in volumes:

                cloud.volume_delete( volume )

            cloud.server_delete( node['id'] )

            instances.set_vm_state(node['id'], 'vm_stopping')
            instances.set_node_state( node['id'], 'node_retiring')

            nr -= 1
            if ( nr <= 0 ):
                return
        else:
            logger.debug("Cannot kill node {} it is {}/{}".format( node_name, node['node_state'],node['vm_state'] ))



    return




def create_execute_nodes(instances, config:Munch, execute_config_file:str=None, nr:int=1):
    """ Create a number of execute nodes

    Args:
       config: config settings
       config_file: config file for node
       nr: nr of nodes to spin up (default 1)

    Returns:
      None
    
    Raises:
      RuntimeError if unknown node-allocation method
      None
    """

    nr = 1

    node_names = []


    for i in range(0, nr ):

        cloud_name = None
        clouds = list(instances.clouds().keys())

        clouds_usable = []
        
        for cloud_name in clouds:
            cloud = instances.get_cloud( cloud_name )
            resources = cloud.get_resources_available()
            if ( resources['ram']       > config.daemon.min_ram*1024 and
                 resources['cores']     > config.daemon.min_cores and
                 resources['instances'] > config.daemon.min_instances):
                clouds_usable.append( cloud_name )

        clouds = clouds_usable
                
        if ( clouds == []):
            logger.warn('No resources available to make a new node')
            return
        
        # for round-robin
        ### find the next cloud name
        if 'node_allocation' not in config.daemon:
            config.daemon['node_allocation'] = 'round-robin'

        if ( config.daemon.node_allocation == 'round-robin'):

            nodes_created = len( instances.get_nodes())

            if nodes_created == 0:
                cloud_name = clouds[ 0 ]
            else:
                cloud_name = clouds[ nodes_created%len( clouds )]
            
            node_name = make_node_name(config.ehos.project_prefix, "execute")

        elif ( config.ehos.deamon.node_allocation == 'random'):
            cloud_name = random.choice( clouds )
        elif (  config.ehos.deamon.node_allocation == 'fill first'):
            cloud_name = clouds[ 0 ]

        else:
            logger.critical("Unknown node allocation method ({})".format( config.daemon.node_allocation ))
            raise RuntimeError("Unknown node allocation method ({})".format( config.daemon.node_allocation ))


        cloud = instances.get_cloud( cloud_name )

        logger.debug( "Using image {}".format( config.clouds[ cloud_name ].image ))
        
        try:
            config.ehos.image= config.clouds[ cloud_name ].image
            
            node_id = cloud.server_create( name=node_name,
                                           userdata_file=execute_config_file,
                                           **config.ehos )

            if ( 'scratch_size' in config.ehos and
                 config.ehos.scratch_size is not None and
                 config.ehos.scratch_size != 'None'):

                try:
                    volume_id = cloud.volume_create(size=config.ehos.scratch_size, name=node_name)
                    cloud.attach_volume( node_id, volume_id=volume_id)
                except:
                    logger.warning("Could not create execute server, not enough disk available, deleting the instance.")
                    cloud.server_delete( node_id )


            instances.add_node( id=node_id, name=node_name, cloud=cloud_name, node_state='node_starting', vm_state='vm_booting')
            logger.debug("Execute server {}/{} is vm_booting".format( node_id, node_name))
            node_names.append(node_name)

        except Exception as e:
            logger.warning("Could not create execute server")
            logger.debug("Error: {}".format(e))

                            

    return node_names






def create_master_node(instances, config:Munch, master_file:str):
    """ Create a number of images to be used later to create nodes

    Args:
       config: config settings
       master_file: config file for master node


    Returns:
      the master_id (uuid)
    
    Raises:
      RuntimeError if unknown node-allocation method
    """

    clouds = list(instances.clouds().keys())

    if len (clouds ) == 1:
        logger.debug( "only one cloud configured, will use that for the master node regardless of configuration")
        config.daemon.master_cloud = clouds[ 0 ]
        
    if 'master_cloud' not in config.daemon or config.daemon == 'None':
        logger.critical( "Cloud instance for hosting the master node is not specified in the config file (daemon:master_cloud)")
        sys.exit( 2 )
    
    cloud_name = config.daemon.master_cloud

    if ( cloud_name not in clouds):
        print( "Unknown cloud instance {}, it is not found in the config file".format( cloud_name ))

    cloud = instances.get_cloud( cloud_name )

    master_name = make_node_name(config.ehos.project_prefix, "master")

    config.ehos.image = config.clouds[ cloud_name ].image
    
    master_id = cloud.server_create( name=master_name,
                                     userdata_file=master_file,
                                     **config.ehos )

        
    logger.info("Created master node, waiting for it to come online. This can take upto 15 minutes")

    
    # Wait for the server to come online and everything have been configured.    
    cloud.wait_for_log_entry(master_id, "The EHOS vm is up after ", timeout=1300)
    logger.info("Master node is now online")

    logger.info( "Master IP addresse is {}".format( cloud.server_ip( master_id )))
    

    return master_id, cloud.server_ip( master_id )
