#!/usr/bin/env python3
""" 
 
 
 
 Kim Brugger (22 Jan 2021), contact: kim.brugger@uib.nok
"""

import os
import sys
import json
import argparse
import pprint
pp = pprint.PrettyPrinter(indent=4)

sys.path.append(".")
import kbr.run_utils as run_utils
import kbr.version_utils as version_utils

version = None
version = version_utils.as_string()

def file_path(filename:str=None) -> str:
    if filename is None:
        filename = __file__
        
    return os.path.realpath(__file__) 


def file_dir(filename:str=None) -> str:
    return os.path.dirname(file_path(filename))




def main():

    
    parser = argparse.ArgumentParser(description=f'playbook runner wrapper script')

    parser.add_argument('-p', '--playbook', required=False, help="playbook to run", default='ehos_nodes.yml')
    parser.add_argument('hosts', nargs=1, help="host to run on")

    args = parser.parse_args()

    pdir = file_dir() + "/../"
    
#    args.hosts = ",".join(args.hosts) + ","
    args.hosts = args.hosts[0]
    
    cmd = f"ANSIBLE_STDOUT_CALLBACK=ansible.posix.json ./venv/bin/ansible-playbook -i {args.hosts}, -e 'ansible_user=centos' test.yml -e 'dynamic_host={args.hosts}'"
    print( f"Working dir {pdir} command: {cmd}" )
    r = run_utils.launch_cmd(cmd, cwd=pdir)

    # the playbook failed!
    if r.p_status != 0:
        print( r.stderr )
    else:
#        print( r.stdout )
        playbook_log = json.loads(r.stdout)
        pp.pprint( playbook_log )
    

if __name__ == "__main__":
    main()


