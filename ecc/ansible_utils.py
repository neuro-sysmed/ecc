
import json
import pprint as pp

import kbr.run_utils as run_utils

def file_path(filename:str=None) -> str:
    if filename is None:
        filename = __file__

    return os.path.realpath(__file__)


def file_dir(filename:str=None) -> str:
    return os.path.dirname(file_path(filename))


def run_playbook(cmd:str, cwd:str=None):

    cmd = f"ANSIBLE_STDOUT_CALLBACK=ansible.posix.json ANSIBLE_HOST_KEY_CHECKING=False {cmd}"

#    print(cwd, cmd)
    r = run_utils.launch_cmd(cmd, cwd=cwd)

    # the playbook failed!
    if r.p_status != 0:
        print( r.stderr )
        return None

    playbook_log = json.loads(r.stdout)
    return playbook_log

