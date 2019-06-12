#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
from collections import defaultdict
import json,sys
import requests
import argparse
import ConfigParser

import logging
from commonHelpers.logger import logger
logger = logger.getChild("mephisto")

from influxdb import InfluxDBClient
import mysql.connector

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('--skipSubmit', action='store_true', help='do not upload to DB')
args = parser.parse_args()

if args.debug:
    logging.getLogger("mephisto").setLevel(logging.DEBUG)

def run():

    config = ConfigParser.ConfigParser()
    config.read("config.cfg")

    password = config.get("credentials", "password")
    username = config.get("credentials", "username")
    database = config.get("credentials", "database")

    logger.info('Constructing MySQL connector.')

    reader = mysql.connector.connect(user='monit', password=password, host='dbod-sql-graf.cern.ch', port=5501 ,database='monit_jobs')
    read_cursor = reader.cursor()
    writer = mysql.connector.connect(user='monit', password=password, host='dbod-sql-graf.cern.ch', port=5501 ,database='monit_jobs')
    write_cursor = writer.cursor()

    logger.info('Getting existing data.')
    read_cursor.execute("select panda_queue, resource from jobs")

    with open('pandaqueue_scraped.json') as pandaqueue:
        panda_queues = json.load(pandaqueue)

    with open('pandaqueue_actual_map.json') as pandaresource:
        panda_resources = json.load(pandaresource)

    with open('daods_datadisk.json') as datadisks:
        datadisk_info = json.load(datadisks)

    for (panda_queue, resource) in read_cursor:
        try:
            nickname = panda_resources[panda_queue] #do the mapping to actual panda queue nicknames
        except:
            logger.warning("Does not exist: queue: %s    Resource: %s" %(panda_queue, resource))
            continue

        logger.debug("Queue: %s    Resource: %s" %(panda_queue, resource))

        atlas_site = panda_queues[nickname]["atlas_site"]
        type = panda_queues[nickname]["type"]
        cloud = panda_queues[nickname]["cloud"]
        site_state = panda_queues[nickname]["status"]
        tier = panda_queues[nickname]["tier"]

        if "MCORE" in resource:
            if panda_queues[nickname]["corecount"]:
                resource_factor = float(panda_queues[nickname]["corecount"])
            else:
                resource_factor = 8.0
        else:
            resource_factor = 1.0

        ddm_names = panda_queues[nickname]["ddm"].split(",")
        datadisk_names = [d for d in ddm_names if "DATADISK" in d]

        if len(datadisk_names) > 1:
            logger.warning("Got more than one datadisk for: %s, %s" % (atlas_site, datadisk_names))

        try:
            datadisk_name = datadisk_names[0]
            datadisk_size = datadisk_info[datadisk_name]["bytes"]/(1e9)
            datadisk_files = datadisk_info[datadisk_name]["files"]
        except:
            logger.warning("Datadisk not found for: %s, %s" % (atlas_site, datadisk_names))
            datadisk_name = "NONE"
            datadisk_size = 0.0
            datadisk_files = 0

        add_point = ('''INSERT INTO jobs (panda_queue, resource) VALUES ("{panda_queue}", "{resource}") ON DUPLICATE KEY UPDATE atlas_site="{atlas_site}", type="{type}", cloud="{cloud}", site_state="{site_state}", tier="{tier}",resource_factor="{resource_factor}", datadisk_name="{datadisk_name}", datadisk_occupied_gb="{datadisk_size}", datadisk_files="{datadisk_files}"'''.format(
                    atlas_site = atlas_site,
                    panda_queue = panda_queue,
                    type = type,
                    cloud = cloud,
                    site_state = site_state,
                    tier = tier,
                    resource_factor=resource_factor,
                    resource = resource,
                    datadisk_name=datadisk_name,
                    datadisk_size=datadisk_size,
                    datadisk_files=datadisk_files))

        if panda_queue == 'ANALY_SiGNET':
            print(add_point)
            print(atlas_site)

        write_cursor.execute(add_point)

    writer.commit()
    read_cursor.close()
    write_cursor.close()
    reader.close()
    writer.close()


if __name__== "__main__":
    run()
