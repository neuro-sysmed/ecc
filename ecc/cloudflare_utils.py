#!/usr/bin/env python3

import argparse
import os
import sys

from tabulate import tabulate
import CloudFlare
cf = None
DEFAULT_ZONE = None

def init(api_key:str, email:str, zone:str='usegalaxy.no'):
    global DEFAULT_ZONE, cf
    DEFAULT_ZONE = zone
    cf = CloudFlare.CloudFlare(email=email, token=api_key)



def list_records( ):

    zone_name = DEFAULT_ZONE
    
    # query for the zone name and expect only one value back
    try:
        zones = cf.zones.get(params = {'name':zone_name,'per_page':1})
    except CloudFlare.exceptions.CloudFlareAPIError as e:
        exit('/zones.get %d %s - api call failed' % (e, e))
    except Exception as e:
        exit('/zones.get - %s - api call failed' % (e))

    if len(zones) == 0:
        exit('No zones found')

    # extract the zone_id which is needed to process that zone
    zone = zones[0]
    zone_id = zone['id']

    page = 1
    table = []
    while True:
        # request the DNS records from that zone
        try:
            dns_records = cf.zones.dns_records.get(zone_id, params={'page':page, 'page_size':20})
        except CloudFlare.exceptions.CloudFlareAPIError as e:
            exit('/zones/dns_records.get %d %s - api call failed' % (e, e))

        if dns_records == [] or dns_records is None:
            break

        for dns_record in dns_records:
            table.append( dns_record )

        page += 1


    return table


def delete_record( dns_record_id:str ) -> None:

    zone_name = DEFAULT_ZONE


    zone_info = cf.zones.get(params={'name': zone_name})[0]
    zone_id = zone_info['id']
#    print( zone_id )
    
    r = cf.zones.dns_records.delete(zone_id, dns_record_id)
    return r

def add_record( r_type, r_name, r_value, r_ttl:int=1000 ) -> None:


    zone_name = DEFAULT_ZONE
 #   print(f"ZN :: {zone_name}")
 #   print( cf.zones.get(params={'name': zone_name}) )

    zone_info = cf.zones.get(params={'name': zone_name})[0]
    zone_id = zone_info['id']
    data = {"type": r_type, "name": r_name, "content": r_value, "ttl": r_ttl}
    if r_type == "MX":
        data["priority"] = 10
    try:

        r = cf.zones.dns_records.post(zone_id, data=data)
    except Exception as e:
        print(e)

    return r


def purge_name(hostname):
    for entry in list_records():
        if entry['name'] == hostname:
            delete_record(entry['id'])