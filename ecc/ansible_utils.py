
import json
import pprint as pp
import os

import kbr.run_utils as run_utils
import kbr.log_utils as logger

def file_path(filename:str=None) -> str:
    if filename is None:
        filename = __file__

    return os.path.realpath(__file__)


def file_dir(filename:str=None) -> str:
    return os.path.dirname(file_path(filename))


def run_playbook(cmd:str, cwd:str=None):

    cmd = f"ANSIBLE_STDOUT_CALLBACK=ansible.posix.json ANSIBLE_HOST_KEY_CHECKING=False {cmd}"

    logger.debug(f"Working die: {cwd}, CMD: {cmd}")
    r = run_utils.launch_cmd(cmd, cwd=cwd, use_shell_env=True)

    # the playbook failed!
    if r.p_status != 0:
        logger.critical( "Playbook failed" )
        logger.critical( r.stderr )
        raise RuntimeError

    playbook_log = json.loads(r.stdout)
    return playbook_log

