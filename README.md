# ECC
**E**lastic **C**loud **C**compute for usegalaxy.no

ECC is a cloud based HPC where the number of compute nodes that
scales according to a workload.  This ensures that resources are not
wasted when during computational lulls.

ECC is implemented in python 3, and extends the usegalaxy.no slurm cluster

## Installation

## Manual configuratino and running EHOS daemon

Install the usegalaxy.no infrastructure-playbook according to that documentation.

```bash
# With the virtual environment in the infrastructure-playbook
# Install ecc
pip install git+https://github.com/usegalaxy-no/ecc.git

# Make a ssh-key for the user who will be running the daemon (no passphrase)
ssh-keygen 

# add ecc user with sudo access to head-node and slurm-node, 
useradd ecc
# sudo entry
# User rules for ecc
ecc ALL=(ALL) NOPASSWD:ALL

# add generated sshkey to ecc/.ssh/autohorized_keys


# make a config file:
ecc-cli init
# edit in password and suitable paths

# fetch the cloud-init sample file:
wget -O ecc_node.yaml https://raw.githubusercontent.com/usegalaxy-no/ecc/master/ecc_node.yaml.sample 

# Add ssh-key generated above

# run daemon 
eccd.py -c ecc.yaml

# run cli
ecc-cli -c ecc.yaml help


```

