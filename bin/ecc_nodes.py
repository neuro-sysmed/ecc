#!/home/brugger/projects/usegalaxy/ecc/venv/bin/python3
# !/bin/env python3
#
#
#
#
# Kim Brugger (20 Sep 2018), contact: kim@brugger.dk

import sys
import configparser
import os
import pprint as pp


import argparse
try:
    import json
except ImportError:
    import simplejson as json

def file_dir(filename:str=None) -> str:
    if filename is None:
        filename = __file__

    return os.path.dirname(os.path.realpath(filename))


sys.path.append(file_dir() +'/../')


import kbr.config_utils as config_utils
import kbr.log_utils as logger


import ecc
import ecc.utils








def readin_inventory(ansible_dir:str) -> {}:

    inventory = f"{ansible_dir}/hosts   "

    if os.path.isfile(f"{ansible_dir}/ansible.cfg"):
        config = configparser.ConfigParser()
        config.read(f"{ansible_dir}/ansible.cfg")
        if 'defaults' in config and 'inventory' in config['defaults']:
            inventory = f"{ansible_dir}/{config['defaults']['inventory']}"



    try:
        config = configparser.ConfigParser()
        config.read(inventory)
    except:
        print('could not find or open the inventory file')
        sys.exit(-1)


    hosts = {f"slurm":{"hosts":[]}, "_meta": {"hostvars":{}}}


    for section in config.sections():
        hosts[section] = {}
        hosts[section]["hosts"] = []
        for key in config[section].keys():
            line = f"{key}={''.join(config[section][key])}"

            fields = line.split()
            host = fields[ 0 ]
            hosts[section]["hosts"].append( host )
            hosts["_meta"]['hostvars'][host] = {}
            for f in fields[1:]:
                key, value = f.split("=")
                hosts["_meta"]['hostvars'][host][key] = value


    return hosts

def main():

    parser = argparse.ArgumentParser(description='ehos_status: print ehos status in telegraf format')
#    parser.add_argument('config_file', metavar='config-file', nargs="*", help="yaml formatted config file",
#                        default=ecc.utils.find_config_file('ecc.yaml'))
    parser.add_argument('config_file', metavar='config-file', nargs="*", help="yaml formatted config file",
                        default='ecc.yaml')
    parser.add_argument('--list', action='store_true') # expected by ansible
    parser.add_argument('-H','--host-group', default='slurm', help='host group to put the nodes in') # expected by ansible
    parser.add_argument('-u','--ansible-user', default='centos', help='host group to put the nodes in') # expected by ansible
    parser.add_argument('-t','--trusted-host', default='yes', help='host group to put the nodes in') # expected by ansible

    args = parser.parse_args()



    config = config_utils.readin_config_file(args.config_file)

    logger.init(name='ecc_nodes', log_file=None)
    logger.set_log_level(0)

    hosts = readin_inventory(config.ecc.ansible_dir)

    config.ecc.name_regex = config.ecc.name_template.format("([01-99])")
    ecc.openstack_connect(config.openstack)
    nodes = ecc.servers(config.ecc.name_regex)


    # get the current nodes
    #instances.update( condor.nodes() )
    #nodes = instances.node_state_counts()
    if args.host_group not in hosts:
        hosts[args.host_group] = {"hosts":[]}
    for node in nodes:
#        print( node )
        if len( node['ip']) == 0:
            continue
        ip_addr = node['ip'][0]
        node_name = node['name']
        hosts[f"{args.host_group}"]['hosts'].append( node_name )
        hosts["_meta"]['hostvars'][node_name] = {'ansible_host': ip_addr,
                                                 'ansible_user':args.ansible_user,
                                                 'trusted_host': args.trusted_host}


#    pp.pprint(  hosts )
    print( json.dumps( hosts ))

if __name__ == '__main__':
    main()
else:
    print("Not to be run as a library")
    sys.exit( 1 )

