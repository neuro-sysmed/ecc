#!/usr/bin/python3
""" 
 Functions related to openstack, extends the vm-api
 
 
 
 Kim Brugger (19 Oct 2018), contact: kim@brugger.dk
"""

import sys
import os
import re
import pprint

pp = pprint.PrettyPrinter(indent=4)
import time

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.common.credentials import ServicePrincipalCredentials


import kbr.log_utils as logger




class Azure(object):

    def __init__(self):

        self._backend = "azure"
        self._connection = None

    def check_connection(self): # done, needs testing

        if self._resource_client is None:
            logger.critical("No connection to azure cloud")
            raise ConnectionError

    def connect(self, Subscription_Id:str): # done

        self._credential = AzureCliCredential()
        self._subscription_id = Subscription_Id
        self._resource_client = ResourceManagementClient(self._credential, Subscription_Id)
        self._compute_client  = ComputeManagementClient(self._credential, Subscription_Id)
        self._network_client  = NetworkManagementClient(self._credential, Subscription_Id)

        logger.debug("Connected to openstack server")

    def server_create(self, name: str, image: str, vm_size:str, network: str, **kwargs):
      #Done, needs testing and popping args in correctly

      vm_size = "Standard_D2_v2"

      admin_username = "brugger"
      admin_password = "AUKWAso4P5G2IbZ"

      compute_group_name    = "FOR-NEURO-SYSMED-UTV-COMPUTE"
      network_group_name    = "FOR-NEURO-SYSMED-UTV-NETWORK"

      virtual_network_name  = "FOR-NEURO-SYSMED-UTV-VNET"
      virtual_subnet_name   = "WorkloadsSubnet"
      

      interface_name = f"{name}-eth0"

      network_interface = network_client.network_interfaces.begin_create_or_update(
                     network_group_name,
                     interface_name,
                     { 'location': "westeurope",
                       'ip_configurations': [{
                        'name': f'{name}IPconfig',
                        'subnet': {
                          'id': f"/subscriptions/{self._subscription_id}/resourceGroups/{network_group_name}/providers/Microsoft.Network/virtualNetworks/{virtual_network_name}/subnets/{subnet}",
                        }}
                        ]
                      } 
                    ).result()


      print( network_interface )

      vm = compute_client.virtual_machines.begin_create_or_update(
                compute_group_name,
                name,
                { "location": "westeurope",
                  "hardware_profile": {
                    "vm_size": vm_size
                  },
                  'linux_configuration': {'disable_password_authentication': False,
                                          'patch_settings': {'patch_mode': 'ImageDefault'},
                                          'provision_vm_agent': True},
                  "storage_profile": {
                    "image_reference": {
                      #? "id": "/subscriptions/5a9e26a0-6897-44d6-963e-fae2a2061f27/resourceGroups/FOR-NEURO-SYSMED-UTV-COMPUTE/providers/Microsoft.Compute/images/circ-rna-v1-img"
                      "sku": "8_3",
                      "publisher": "Openlogic",
                      "version": "latest",
                      "offer": "centos"
                    },
                    "os_disk": {
                      "caching": "ReadWrite",
                      "managed_disk": {
                        "storage_account_type": "Standard_LRS"
                      },
                      "name": f"{name}-disk",
                      "create_option": "FromImage"
                    },
                  },
                  "os_profile": {
                    "admin_username": admin_username,
                    "admin_password": admin_password,
                    "computer_name": f"{name}"
                  },
                  "network_profile": {
                    "network_interfaces": [
                      {"id": f"/subscriptions/{self._subscription_id}/resourceGroups/{network_group_name}/providers/Microsoft.Network/networkInterfaces/{interface_name}",
                      "properties": {
                        "primary": True
                      }
                    } 
                  ]
                }
              }
    ).result()

      return vm.id

    def servers(self) -> list: # Done and tested!

        servers = []

        vm_list = self._compute_client.virtual_machines.list_all()

        for vm_general in vm_list:
          general_view = vm_general.id.split("/")
          resource_group = general_view[4]
          vm_name = general_view[-1]
          vm = self._compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')

          codes = []
          power_state = "unknown"
          provisioning_state = "unknown"
          for stat in vm.instance_view.statuses:
            k,v = stat.code.split("/")
            if k == 'PowerState':
              power_state = v
            elif k == 'ProvisioningState':
              provisioning_state = v

          servers.append({'id': vm.id, 'name': vm.name.lower(), 'status': power_state})
          
        logger.debug("Servers: \n{}".format(pp.pformat(servers)))
        return servers


    def server(self, id:str): # done, needs  testing

        return self._compute_client.virtual_machines.get( id )


    def server_names(self) -> []: # done
        names = []
        for server in self.servers():
            names.append(server['name'])

        return names

    def server_delete(self, id: str, compute_group:str, **kwargs):

#        self._compute_client.virtual_machines.begin_delete('FOR-NEURO-SYSMED-UTV-COMPUTE', 'kbr-api-test')
        self._compute_client.virtual_machines.begin_delete(resource_group, name)


    def server_stop(self, id: str, compute_group:str, **kwargs): # done, needs testing
        """ stops a server """

        self._compute_client.virtual_machines.power_off(compute_group, id)

        logger.debug("Suspended server id:{}".format(id))


    def get_images(self, compute_group:str, name:str=None, **kwargs):

        images = []
        for image in self._compute_client. _connection.image.images.list():
            if active and image.status != "active":
                continue

            if (name is not None and
               (name.lower() not in image.name.lower() and
                name.lower() not in image.id.lower())):
                continue

            image_info = {'id': image.id,
                          'name': image.name,
                          'tags': image.tags}

            images.append(image_info)

        return images

