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

sys.path.append(".")
import ecc
import ecc.slurm_utils as slurm_utils
import ecc.cloudflare_utils as cloudflare_utils
import ecc.ansible_utils as ansible_utils


config = None

def init(config: Munch):
    logger.init(name='eccd', log_file=config.get('logfile', None))
    logger.set_log_level(config.ecc.log_level)
    config.ecc.name_regex = config.ecc.name_template.format("(\d+)")
    ecc.set_config( config )
    ecc.openstack_connect(config.openstack)
    cloudflare_utils.init(config.ecc.cloudflare_apikey, config.ecc.cloudflare_email)
    #cloudflare_utils.add_record('A', 'ecc10.usegalaxy.no', '158.39.77.46', 1000)
#    cloudflare_utils.purge_name('ecc1.usegalaxy.no')
#    sys.exit()
    ansible_utils.run_playbook(config.ecc.ansible_cmd, host="158.39.201.200", cwd=config.ecc.ansible_dir)
#    sys.exit()



def run_daemon() -> None:
    """ Creates the ecc daemon loop that creates and destroys nodes etc.
    """

    while (True):

        # get the current number of nodes and jobs
        nodes_total = slurm_utils.nodes_total()
        nodes_idle = slurm_utils.nodes_idle()
        jobs_pending = slurm_utils.jobs_pending()

        print(f"nodes_total: {nodes_total}, nodes_idle: {nodes_idle}, jobs_pending: {jobs_pending}")

        print( config )

        # Below the min number of nodes needed for our setup
        if nodes_total < config.ecc.nodes_min:
            logger.info("We are below the min number of nodes, creating {} nodes".format(
                config.ecc.nodes_min - nodes_total))

            node_names = ecc.create_node(cloud_init_file=config.ecc.cloud_init)

        ### there are jobs queuing, let see what we should do

        # got jobs in the queue but less than or equal to our idle + spare nodes, do nothing
        elif jobs_pending and jobs.job_idle <= nodes_idle:
            logger.info("We got stuff to do, but seems to have excess nodes to cope...")

            nr_of_nodes_to_delete = min(nodes_total - config.ecc.nodes_min, nodes_idle - jobs_pending,
                                        nodes_idle - config.ecc.nodes_spare)

            logger.info("Deleting {} idle nodes... (1)".format(nr_of_nodes_to_delete))
            ecc.delete_idle_nodes(instances, condor.nodes(), nr_of_nodes_to_delete)


        # Got room to make some additional nodes
        elif (jobs_pending and nodes_idle == 0 and nodes_total <= config.ecc.nodes_max):

            logger.info("We got stuff to do, creating some additional nodes...")

            node_names = ecc.create_execute_nodes(instances, config, config.ecc.nodes_max - nodes_total,
                                                  cloud_init_file=config.ecc.cloud_init_file)
            log_nodes(node_names)


        # this one is just a sanity one
        elif (jobs.job_idle and nodes_total == config.ecc.nodes_max):
            logger.info("We are busy. but all nodes we are allowed have been created, nothing to do")


        elif (jobs.job_idle):
            logger.info("We got stuff to do, but seems to have nodes to cope...")


        ### Looks like we have an excess of nodes, lets cull some

        # We got extra nodes not needed and we can delete some without going under the min cutoff, so lets get rid of some
        elif (nodes_total > config.ecc.nodes_min and
              nodes_idle > config.ecc.nodes_spare):

            nr_of_nodes_to_delete = min(nodes_total - config.ecc.nodes_min,
                                        nodes_idle - config.ecc.nodes_spare)

            logger.info("Deleting {} idle nodes... (2)".format(nr_of_nodes_to_delete))
            ecc.delete_idle_nodes(instances, condor.nodes(), nr_of_nodes_to_delete)

        else:
            logger.info("The number of execute nodes are running seem appropriate, nothing to change.")

        logger.info("Napping for {} seccnd(s).".format(config.ecc.sleep))
        time.sleep(config.ecc.sleep)


def main():
    #    print( slurm_utils.nodes_idle() )
    #    print( slurm_utils.nodes_total() )

    parser = argparse.ArgumentParser(description='eccd: the ecc daemon to be run on the master node ')
    parser.add_argument('-l', '--logfile', default=None, help="Logfile to write to, default is stdout")
    parser.add_argument('-v', '--verbose', default=4, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('config_file', metavar='config-file', nargs=1, help="yaml formatted config file",
                        default=ecc.utils.find_config_file('ecc.yaml'))

    args = parser.parse_args()

    global config
    config = config_utils.readin_config_file(args.config_file[0])
    config.ecc.log_level = args.verbose
    init(config)
    run_daemon()


if __name__ == '__main__':
    main()
else:
    print("Not to be run as a library")
    sys.exit(1)
