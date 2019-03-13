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

import mysql.connector

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument('--debug', action='store_true', help='print debug messages')
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
        site_state = panda_queues[nickname]["state"]
        tier = panda_queues[nickname]["tier"]


        add_point = ('''INSERT INTO jobs (atlas_site, panda_queue, resource) VALUES ("{atlas_site}", "{panda_queue}", "{resource}") ON DUPLICATE KEY UPDATE type="{type}", cloud="{cloud}", site_state="{site_state}", tier="{tier}"'''.format(
                    atlas_site = atlas_site,
                    panda_queue = panda_queue,
                    type = type,
                    cloud = cloud,
                    site_state = site_state,
                    tier = tier,
                    resource = resource))

        write_cursor.execute(add_point)

    # client.write_points(points=points_list, time_precision="n")

    writer.commit()
    read_cursor.close()
    write_cursor.close()
    reader.close()
    writer.close()


if __name__== "__main__":
    run()
