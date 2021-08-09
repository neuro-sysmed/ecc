#!/usr/bin/env python3
# 
# 
# 
# 

import pprint
import sys

pp = pprint.PrettyPrinter(indent=4)
import time
import argparse
from munch import Munch

import kbr.config_utils as config_utils
import kbr.log_utils as logger
import kbr.version_utils as version_utils

sys.path.append(".")
import ecc
import ecc.slurm_utils as slurm_utils
import ecc.cloudflare_utils as cloudflare_utils
import ecc.ansible_utils as ansible_utils


version = version_utils.as_string('ecc')
config = None
program_name = 'eccd'


def init(args):
    global config
    if args.config:
        config = config_utils.readin_config_file(args.config[0])
        logger.init(name=program_name, log_file=config.ecc.get('logfile', None))
        logger.set_log_level(args.verbose)
        logger.info(f'{program_name} (v:{version})')
        config.ecc.name_regex = config.ecc.name_template.format("(\d+)")
        ecc.set_config(config)
        ecc.openstack_connect(config.openstack)
        cloudflare_utils.init(config.ecc.cloudflare_apikey, config.ecc.cloudflare_email)
    else:
        logger.init(name=program_name)
        logger.set_log_level(args.verbose)
        logger.info(f'{program_name} (v:{version})')



def run_daemon() -> None:
    """ Creates the ecc daemon loop that creates and destroys nodes etc.
    """

    while (True):

        # get the current number of nodes and jobs
        ecc.update_nodes_status()

        nodes_total = ecc.nodes_total()
        nodes_idle = ecc.nodes_idle()
        jobs_pending = slurm_utils.jobs_pending()

        print(f"nodes_total: {nodes_total}, nodes_idle: {nodes_idle}, jobs_pending: {jobs_pending}")

        # Below the min number of nodes needed for our setup
        if nodes_total < config.ecc.nodes_min:
            logger.info("We are below the min number of nodes, creating {} nodes".format(
                config.ecc.nodes_min - nodes_total))

            ecc.create_nodes(cloud_init_file=config.ecc.cloud_init, count=config.ecc.nodes_min - nodes_total)

        ### there are jobs queuing, let see what we should do

        # Got room to make some additional nodes
        elif (jobs_pending and nodes_idle == 0 and nodes_total <= int(config.ecc.nodes_max)):

            logger.info("We got stuff to do, creating some additional nodes...")

            ecc.create_nodes(cloud_init_file=config.ecc.cloud_init, count=1)

        # We got extra nodes not needed and we can delete some without going under the min cutoff, so lets get rid of some
        elif jobs_pending == 0 and nodes_idle and nodes_total > config.ecc.nodes_min:

            nr_of_nodes_to_delete = nodes_total - int(config.ecc.nodes_min)

            logger.info(f"Deleting {nr_of_nodes_to_delete} idle nodes... ")
            ecc.delete_idle_nodes(nr_of_nodes_to_delete)

        else:
            logger.info("Nothing to change.")

        logger.info("Napping for {} second(s).".format(config.ecc.sleep))
        time.sleep(config.ecc.sleep)


def main():

    parser = argparse.ArgumentParser(description='eccd: the ecc daemon to be run on the master node ')
    parser.add_argument('-l', '--logfile', default=None, help="Logfile to write to, default is stdout")
    parser.add_argument('-v', '--verbose', default=4, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('config', metavar='config', nargs=1, help="yaml formatted config file",
                        default=ecc.utils.find_config_file('ecc.yaml'))

    args = parser.parse_args()
    init(args)
    run_daemon()


if __name__ == '__main__':
    main()
else:
    print("Not to be run as a library")
    sys.exit(1)
