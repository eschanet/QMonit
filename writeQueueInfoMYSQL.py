#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
from collections import defaultdict
import json, sys

import requests

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

import argparse
import ConfigParser

import logging
from commonHelpers.logger import logger

logger = logger.getChild("mephisto")

from influxdb import InfluxDBClient
import mysql.connector

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument("--debug", action="store_true", help="print debug messages")
parser.add_argument("--skipSubmit", action="store_true", help="do not upload to DB")
args = parser.parse_args()

if args.debug:
    logging.getLogger("mephisto").setLevel(logging.DEBUG)


def run():

    config = ConfigParser.ConfigParser()
    config.read("config.cfg")

    password = config.get("credentials", "password")
    username = config.get("credentials", "username")
    database = config.get("credentials", "database")

    logger.info("Constructing MySQL connector.")

    reader = mysql.connector.connect(
        user="monit",
        password=password,
        host="dbod-sql-graf.cern.ch",
        port=5501,
        database="monit_jobs",
    )
    read_cursor = reader.cursor()
    writer = mysql.connector.connect(
        user="monit",
        password=password,
        host="dbod-sql-graf.cern.ch",
        port=5501,
        database="monit_jobs",
    )
    write_cursor = writer.cursor()

    logger.info("Getting existing data.")
    read_cursor.execute("select panda_queue,prod_source, resource from jobs")

    def getJSON(file):
        with open(file) as f:
            return json.load(f)

    panda_queues = getJSON("data/scraped_cric_pandaqueue.json")
    panda_resources = getJSON("data/map_PQ_names.json")
    datadisk_info = getJSON("data/scraped_grafana_datadisk.json")
    federations_resources = getJSON("data/scraped_rebus_federations.json")

    for (panda_queue, prod_source, resource) in read_cursor:
        try:
            nickname = panda_resources[
                panda_queue
            ]  # do the mapping to actual panda queue nicknames
            atlas_site = panda_queues[nickname]["atlas_site"]
        except:
            logger.warning(
                "Does not exist: queue: %s   Prod_source: %s    Resource: %s"
                % (panda_queue, prod_source, resource)
            )
            continue

        logger.debug(
            "Queue: %s    Prod_source: %s     Resource: %s"
            % (panda_queue, prod_source, resource)
        )

        atlas_site = panda_queues[nickname]["atlas_site"]
        type = panda_queues[nickname]["type"]
        cloud = panda_queues[nickname]["cloud"]
        country = panda_queues[nickname]["country"]
        federation = panda_queues.get(nickname, {}).get("rc", "None")
        site_state = panda_queues[nickname]["status"]
        tier = panda_queues[nickname]["tier"]
        resource_type = panda_queues[nickname].get("resource_type", "None")

        if "MCORE" in resource:
            if panda_queues[nickname]["corecount"]:
                resource_factor = float(panda_queues[nickname]["corecount"])
            else:
                resource_factor = 8.0
        else:
            resource_factor = 1.0

        ddm_names = (
            panda_queues.get(nickname, {}).get("astorages", {}).get("read_lan", [])
        )

        # ddm_names = panda_queues[nickname]["ddm"].split(",")
        datadisk_names = [d for d in ddm_names if "DATADISK" in d]

        if len(datadisk_names) > 1:
            logger.warning(
                "Got more than one datadisk for: %s, %s" % (atlas_site, datadisk_names)
            )

        try:
            datadisk_name = datadisk_names[0]
            datadisk_size = datadisk_info[datadisk_name]["bytes"] / (1e9)
            datadisk_files = datadisk_info[datadisk_name]["files"]
        except:
            logger.warning(
                "Datadisk not found for: %s, %s" % (atlas_site, datadisk_names)
            )
            datadisk_name = "NONE"
            datadisk_size = 0.0
            datadisk_files = 0

        add_point = '''INSERT INTO jobs (panda_queue, prod_source, resource) VALUES ("{panda_queue}","{prod_source}", "{resource}") ON DUPLICATE KEY UPDATE atlas_site="{atlas_site}", type="{type}", country="{country}", cloud="{cloud}",federation="{federation}", site_state="{site_state}", tier="{tier}",resource_factor="{resource_factor}",resource_type="{resource_type}", datadisk_name="{datadisk_name}", datadisk_occupied_gb="{datadisk_size}", datadisk_files="{datadisk_files}"'''.format(
            atlas_site=atlas_site,
            panda_queue=panda_queue,
            type=type,
            prod_source=prod_source,
            cloud=cloud,
            country=country,
            federation=federation,
            site_state=site_state,
            tier=tier,
            resource_factor=resource_factor,
            resource=resource,
            resource_type=resource_type,
            datadisk_name=datadisk_name,
            datadisk_size=datadisk_size,
            datadisk_files=datadisk_files,
        )

        if panda_queue == "ANALY_SiGNET":
            print(add_point)
            print(atlas_site)

        write_cursor.execute(add_point)

    writer.commit()
    read_cursor.close()
    write_cursor.close()
    reader.close()
    writer.close()


if __name__ == "__main__":
    run()
