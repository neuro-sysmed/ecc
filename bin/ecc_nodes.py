#!/usr/bin/python3
#
#
#
#
# Kim Brugger (20 Sep 2018), contact: kim@brugger.dk

import sys


import argparse
from munch import Munch
try:
    import json
except ImportError:
    import simplejson as json


import kbr.config_utils as config_utils
import kbr.log_utils as logger

sys.path.append('.')

import ecc
import ecc.utils

def main():

    parser = argparse.ArgumentParser(description='ehos_status: print ehos status in telegraf format')
    parser.add_argument('config_file', metavar='config-file', nargs="*", help="yaml formatted config file",
                        default=ecc.utils.find_config_file('ecc.yaml'))
    parser.add_argument('--list', action='store_true') # expected by ansible
    parser.add_argument('-H','--host-group', default='nodes', help='host group to put the nodes in') # expected by ansible
    args = parser.parse_args()



    config = config_utils.readin_config_file(args.config_file)

    logger.init(name='ecc_nodes', log_file=None)
    logger.set_log_level(0)


    config.ecc.nodes.name_regex = config.ecc.nodes.name_template.format("([01-99])")
    ecc.openstack_connect(config.openstack)
    nodes = ecc.servers(config.ecc.nodes.name_regex)


    # get the current nodes
    #instances.update( condor.nodes() )
    #nodes = instances.node_state_counts()
    hosts = {f"{args.host_group}":{"hosts":[]}, "_meta": {"hostvars":{}}}
    for node in nodes:
 
        if len( node['ip']) == 0:
            continue
        ip_addr = node['ip'][0]
        hosts[f"{args.host_group}"]['hosts'].append( ip_addr )
        hosts["_meta"]['hostvars'][ip_addr] = {}
        

    print( json.dumps( hosts ))

if __name__ == '__main__':
    main()
else:
    print("Not to be run as a library")
    sys.exit( 1 )

