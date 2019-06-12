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


def run():

    config = ConfigParser.ConfigParser()
    config.read("config.cfg")

    password = config.get("credentials", "password")
    username = config.get("credentials", "username")
    database = config.get("credentials", "database")


    client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_daod", True, False)
    points_list = []

        # json_body = {
        #     "measurement": "jobs",
        #     "tags": tags,
        #     "time" : time,
        #     "fields" : {
        #         "jobs" : n_jobs,
        #         "resource_factor" : resource_factor
        #     }
        # }
        #
        # points_list.append(json_body)

    # client.write_points(points=points_list, time_precision="n")


if __name__== "__main__":
    run()
