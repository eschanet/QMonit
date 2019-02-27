#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
from datetime import datetime,timedelta
import hashlib
import ConfigParser

from influxdb import InfluxDBClient
import Client

config = ConfigParser.ConfigParser()
config.read("config.cfg")

password = config.get("credentials", "password")
username = config.get("credentials", "username")
database = config.get("credentials", "database")

client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)

print(client.query('show measurements'))
result = client.query('''select * from "10m"."jobs" where "job_status" = 'running' and time > now() - 10m group by * ''')

print(result)
# points_list = []
#
#
# json_body = {   "measurement": "jobs",
#                 "tags": {
#                     "atlas_site": atlas_site,
#                     "panda_queue" : site,
#                     "resource" : core,
#                     "type" : type,
#                     "cloud" : cloud,
#                     "site_state" : site_state,
#                     "job_status" : job_status
#                 },
#                 "time" : time,
#                 "fields" : {
#                     "jobs" : n_jobs,
#                     "resource_factor" : resource_factor
#                 }
#             }
#
# points_list.append(json_body)
#
# client.write_points(points=points_list, time_precision="n")
