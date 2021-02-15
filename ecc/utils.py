import datetime
import os
import random
import re
import shlex
import shutil
import socket
import string
import subprocess
import time
from typing import List, Tuple

from munch import Munch

import kbr.log_utils as logger


def get_host_ip() -> str:
    """ gets the host ip address

    Args:
      None

    returns:
      Host Ip 4 address

    Raises:
      None
    """
    try:
        address = socket.gethostbyname(socket.gethostname())
        # On some system, this always gives me 127.0.0.1. Hence...
    except:
        address = ''

    if not address or address.startswith('127.'):
        # ...the hard way.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))
        address = s.getsockname()[0]

    return address



def get_host_name() -> str:
    """ gets the host name

    Args:
      None

    Returns:
      full host name

    Raises:
      None
    """
    return socket.getfqdn()


def timestamp() -> int:

    return int(time.time())


def datetimestamp() -> str:
    ts = time.time()
    ts = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%dT%H%M%S')

    return ts


def random_string(N:int=10) -> str:
    """ Makes a random string the length of N

    Args:
      length: length of random string, default is 10 chars

    Returns:
      random string (str)

    Raises:
      None
    """

    chars=string.ascii_lowercase + string.ascii_uppercase + string.digits

    res = ""

    for _ in range( N ):
        res += random.SystemRandom().choice(chars)

    return res


def make_node_name(prefix="ehos", name='node') -> str:
    """ makes a nodename with a timestamp in it, eg: prefix-name-datetime

    Furthermore, ensures the name will correspond to a hostname, eg making _ to -, and lowercase letters

    Args:
      prefix: prefix for name
      name: type of node, eg executer

    Returns:
      name generated

    Raises:
      None
    """

    node_name = "{}-{}-{}".format(prefix, name, datetimestamp())
    node_name = re.sub("_","-",node_name)
    node_name = node_name.lower()

    return node_name


def system_call(command:str) -> int:
    """ runs a system command

    Args:
      command to run

    Returns:
      None

    Raises:
      None

    """

    return(subprocess.call(shlex.split( command ), shell=False))


def get_node_id(filename:str='/var/lib/cloud/data/instance-id') -> str:
    """ Cloud init stores the node id on disk so it possible to use this for us to get host information that way

    Ideally this would have been done using the cloud_init library, but that is python2 only, so this is a bit of a hack

    Args:
      filename: should only be changed when testing the function

    Returns:
      node id (string)

    Raises:
      if the instance not found raise a RuntimeError
    """


    if ( not os.path.isfile( filename )):
      raise RuntimeError("instance file ({}) does not exists".format( filename))


    fh = open(filename, 'r')
    id  = fh.readline().rstrip("\n")
    fh.close()

    return id


def get_configuration( config_file:str) -> Munch:

    config = readin_config_file( config_file)
  #  pp.pprint( config )
  #  print( config.daemon.use_db_settings  )

    if 'use_db_settings' in config.daemon and config.daemon.use_db_settings == True:
        logger.info( 'Using the database for config/settings')
        store_config_in_db_if_empty(config.daemon.database, config)
        config = config_from_db( config.daemon.database )

    return config

def get_configurations( config_files:[]) -> Munch:

    config = Munch()

    for config_file in config_files:
        next_config = readin_config_file( config_file)
        # Merge contents of dict2 in dict1
        config.update(next_config)

    if 'use_db_settings' in config.daemon and config.daemon.use_db_settings == True:
        logger.info( 'Using the database for config/settings')
        store_config_in_db_if_empty(config.daemon.database, config)
        config = config_from_db( config.daemon.database )


    return config



def readin_config_file(config_file:str) -> Munch:
    """ reads in and checks the config file

    Args:
      config_file: yaml formatted config files

    Returns:
      config ( munch )

    Raises:
      None
    """

    with open(config_file, 'r') as stream:
        config = Munch.fromYAML(stream)
        stream.close()

    if 'daemon' not in config:
        config[ 'daemon' ] = {}
    if 'hostname' not in config.daemon:
        config.daemon['hostname'] = get_host_name()

    return config


def dict_validation(data:dict, template:dict) -> bool:

    for key in template.keys():
        if key not in data:
            raise KeyError

        if not isinstance(data[key], type(template[key])):
            print("Key value error: Expected {}, got a {}".format( type(template[key]), type(data[key])))
            raise AttributeError

        if isinstance(data[ key ], dict):
            dict_validation(data[ key ], template[ key ])

    return True



def readin_whole_file(filename:str) -> str:
    """ reads in a whole file as a single string

    Args:
      filename: name of file to read

    Returns
      file contents (str)

    Raises:
      None
    """

    with open( filename, 'r') as fh:
        lines = "".join(fh.readlines())
        fh.close()

    return lines


def find_config_file( filename:str, dirs:list=None) -> str:
    """ Depending on the setup the location of config files might change. This helps with this, first hit wins!

    The following location are used by default: /etc/ehos/, /usr/local/etc/ehos, /usr/share/ehos/, configs/

    Args:
      filename: file to find
      dirs: additional list of directories to search through

    Return:
      Full path to the file (str)

    Raises:
      RuntimeError if file not found
    """

    script_dir = os.path.dirname(os.path.abspath( filename))

    default_dirs = ['/etc/ehos/',
                    '/usr/local/etc/ehos',
                    '/usr/local/etc/',
                    "{}/../etc".format( script_dir ),
                    'etc/',
                    'etc/ehos',
                    '/usr/share/ehos/',
                    '/usr/local/share/ehos/',
                    "{}/../share".format( script_dir ),
                    'share/',
                    'share/ehos',
                    './']


    if dirs is not None:
        dirs += default_dirs
    else:
#        print( "No additional dirs!")
        dirs = default_dirs

    for dir in dirs:
        full_path = "{}/{}".format( dir, filename)
        if os.path.isfile(full_path):
            return os.path.normpath(full_path)

    raise RuntimeError("File {} not found".format( filename ))


def patch_file(filename:str, pattern:str=None, replace:str=None, patterns:List[ Tuple[str, str]]=None, outfile:str=None):
    """ Alter a file by searching for a pattern (str or regex) and replace it

    The function further more make a backup copy of the original file

    Args:
      filename: The file to change
      pattern: the pattern (str/regex) to search for in the file
      replace: what to replace the pattern with
      patterns: a list of replacements to be done
      outfile: alternative file to write to, will not create a backup of the original file

    Returns:
      None

    Raises:
      RuntimeError if no pattern

    """

    # this is foobared but I cannot find the strength to do this right
    if ( pattern is None and
         replace is None and
         patterns is None):


        #  or

        #  ( patterns is not None and
        #    (pattern is not None or
        #     replace is not None))

        # or

        #  ( pattern is None or
        #    replace is None )):

        raise RuntimeError('Wrong use of alter_file function parameters, provide either a pattern and a replace or a list of patterns')

    # first make a copy of the file

    if ( outfile is None ):
        shutil.copy( filename, "{}.original".format( filename ))

    # open and read in the while file as a single string
    with open( filename, 'r') as fh:
        lines = "".join(fh.readlines())
        fh.close()

    if patterns is None:
        patterns = [ (pattern, replace) ]

    for pattern, replace in patterns:
#        print( pattern, " ---> ", replace )
        # replace the pattern with the replacement string
        lines = re.sub(pattern, replace, lines)

    if ( outfile is None ):
        with  open(filename, 'w') as fh:
            fh.write( lines )
            fh.close()
    else:
        with  open(outfile, 'w') as fh:
            fh.write( lines )
            fh.close()

