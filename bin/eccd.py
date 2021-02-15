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
import ecc.utils
import ecc.slurm_utils as slurm_utils


def init(config: Munch):
    logger.init(name='eccd', log_file=config.get('logfile', None))
    logger.set_log_level(config.ecc.daemon.log_level)
    config.ecc.nodes.name_regex = config.ecc.nodes.name_template.format("([01-99])")
    ecc.set_config( config )
    ecc.openstack_connect(config.openstack)


def run_daemon(config_files: [] = ["/usr/local/etc/ecc.yaml"], cloud_init: str = None,
               ansible_playbook: str = None) -> None:
    """ Creates the ecc daemon loop that creates and destroys nodes etc.
    """

    config = ecc.utils.get_configurations(config_files)

    while (True):

        # get the current number of nodes and jobs
        nodes_total = slurm_utils.nodes_total()
        nodes_idle = slurm_utils.nodes_idle()
        jobs_pending = slurm_utils.jobs_pending()

        # Below the min number of nodes needed for our setup
        if nodes_total < config.daemon.nodes_min:
            logger.info("We are below the min number of nodes, creating {} nodes".format(
                config.daemon.nodes_min - nodes.node_total))

            node_names = ecc.create_nodes(instances, config, nr=config.daemon.nodes_min - nodes.node_total,
                                          execute_config_file=cloud_init)

        ### there are jobs queuing, let see what we should do

        # got jobs in the queue but less than or equal to our idle + spare nodes, do nothing
        elif jobs_pending and jobs.job_idle <= nodes.node_idle:
            logger.info("We got stuff to do, but seems to have excess nodes to cope...")

            nr_of_nodes_to_delete = min(nodes.node_total - config.daemon.nodes_min, nodes.node_idle - jobs.job_idle,
                                        nodes.node_idle - config.daemon.nodes_spare)

            logger.info("Deleting {} idle nodes... (1)".format(nr_of_nodes_to_delete))
            ecc.delete_idle_nodes(instances, condor.nodes(), nr_of_nodes_to_delete)


        # Got room to make some additional nodes
        elif (jobs_pending and nodes_idle == 0 and nodes_total <= config.daemon.nodes_max):

            logger.info("We got stuff to do, creating some additional nodes...")

            node_names = ecc.create_execute_nodes(instances, config, config.daemon.nodes_max - nodes.node_total,
                                                  execute_config_file=cloud_init)
            log_nodes(node_names)


        # this one is just a sanity one
        elif (jobs.job_idle and nodes.node_total == config.daemon.nodes_max):
            logger.info("We are busy. but all nodes we are allowed have been created, nothing to do")


        elif (jobs.job_idle):
            logger.info("We got stuff to do, but seems to have nodes to cope...")


        ### Looks like we have an excess of nodes, lets cull some

        # We got extra nodes not needed and we can delete some without going under the min cutoff, so lets get rid of some
        elif (nodes.node_total > config.daemon.nodes_min and
              nodes.node_idle > config.daemon.nodes_spare):

            nr_of_nodes_to_delete = min(nodes.node_total - config.daemon.nodes_min,
                                        nodes.node_idle - config.daemon.nodes_spare)

            logger.info("Deleting {} idle nodes... (2)".format(nr_of_nodes_to_delete))
            ecc.delete_idle_nodes(instances, condor.nodes(), nr_of_nodes_to_delete)

        else:
            logger.info("The number of execute nodes are running seem appropriate, nothing to change.")

        logger.info("Napping for {} seccnd(s).".format(config.daemon.sleep))
        time.sleep(config.daemon.sleep)


def main():
    #    print( slurm_utils.nodes_idle() )
    #    print( slurm_utils.nodes_total() )

    parser = argparse.ArgumentParser(description='eccd: the ecc daemon to be run on the master node ')
    parser.add_argument('-c', '--cloud-init', help="cloud-init file to run when creating the VM")
    parser.add_argument('-a', '--ansible-playbook', help="ansible-playbook to run on newly created VMs")
    parser.add_argument('-l', '--logfile', default=None, help="Logfile to write to, default is stdout")
    parser.add_argument('-v', '--verbose', default=4, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('config_file', metavar='config-file', nargs=1, help="yaml formatted config file",
                        default=ecc.utils.find_config_file('ecc.yaml'))

    args = parser.parse_args()

    config = config_utils.readin_config_file(args.config_file[0])
    config.ecc.daemon.log_level = args.verbose
#    pp.pprint(config)
    init(config)
    print( slurm_utils.nodes() )
    print(ecc.servers(config.ecc.nodes.name_regex))
    ecc.update_nodes_status()
    ecc.nodes_idle()
    print( "Idle nodes:", ecc.nodes_idle())
    print( "Total nodes:", ecc.nodes_total())
#    run_daemon(args.config_files, cloud_init=args.cloud_init, ansible_playbook=args.ansible_playbook)


if __name__ == '__main__':
    main()
else:
    print("Not to be run as a library")
    sys.exit(1)
