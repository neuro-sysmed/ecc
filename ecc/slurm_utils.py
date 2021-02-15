import re

import kbr.run_utils as run_utils



def jobs():
    #"%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"
    #JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
    #33187 usegalaxy     test sysadmin  R       0:15      1 slurm.usegalaxy.no
    cmd = "squeue -h"

#    run = run_utils.launch_cmd( cmd )

    run = "  33187 usegalaxy     test sysadmin  PD       0:15      1 slurm.usegalaxy.no\n 33187 usegalaxy     test sysadmin  R       0:15      1 slurm.usegalaxy.no"

    jobs = []

    for line in run.split("\n"):
        fields = line.split()
        jobs.append({"id": fields[0], "user": fields[3], "state":fields[4], "time": fields[5]})

    return jobs

def jobs_pending():
    count = 0
    for job in jobs():
        if job['state'] == 'PD' or job['state'] == 'PENDING':
            count += 1

    return count

def jobs_running():
    count = 0
    for job in jobs():
        if job['state'] == 'R' or job['state'] == 'RUNNING':
            count += 1

    return count

def job_counts_by_state():
    counts = {}
    for job in jobs():

        if job['state'] not in counts:
            counts[ job['state'] ] = 1
        else:
            counts[ job['state'] ] += 1

    return counts



def nodes():
    #PARTITION             AVAIL  TIMELIMIT  NODES  STATE NODELIST
    #usegalaxy_production*    up   infinite      2    mix nrec1.usegalaxy.no,slurm.usegalaxy.no
    #usegalaxy_production*    up   infinite      1   idle nrec2.usegalaxy.no
    #State of the nodes.  Possible states include: allocated, completing, down, drained, draining, fail, failing, future, idle, maint, mixed, perfctrs, power_down, power_up, reserved, and unknown plus Their abbreviated forms: alloc, comp, down, drain, drng, fail, failg, futr, idle, maint, mix, npc,  pow_dn,  pow_up,
    #               resv, and unk respectively.  Note that the suffix "*" identifies nodes that are presently not responding.
    cmd = "sinfo -h"

    run = run_utils.launch_cmd( cmd )

    run = " usegalaxy_production*    up   infinite      2    mix nrec1.usegalaxy.no,slurm.usegalaxy.no\n  usegalaxy_production*    up   infinite      1   alloc nrec2.usegalaxy.no"
    nodes = []

    for line in run.split("\n"):
        fields = line.split()
#        print(fields)
        for node in fields[5].split(","):
            nodes.append( {'name':node, 'avail': fields[2], "state": fields[4]})

    return nodes


def nodes_idle():
    count = 0
    for node in nodes():
        if node['state'] in ['mix', 'idle']:
            count += 1

    return count


def nodes_total():
    count = 0
    for _ in nodes():
        count += 1

    return count





