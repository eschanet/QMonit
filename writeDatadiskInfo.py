#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys

import requests
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

import cPickle as pickle
from datetime import datetime,timedelta
import hashlib
import ConfigParser

import logging
from commonHelpers.logger import logger
logger = logger.getChild("mephisto")

from influxdb import InfluxDBClient
import Client

from influxdb import InfluxDBClient
import mysql.connector

config = ConfigParser.ConfigParser()
config.read("config.cfg")

password = config.get("credentials", "password")
username = config.get("credentials", "username")
database = config.get("credentials", "database")

def customfloat(f):
    if f:
        return float(f)
    else:
        return 0.0

def getJSON(file):
    with open(file) as f:
        return json.load(f)

panda_queues = getJSON('data/scraped_cric_pandaqueue.json')
panda_resources = getJSON('data/map_PQ_names.json')
site_resources = getJSON('data/scraped_cric_sites.json')
ddm_resources = getJSON('data/scraped_cric_ddm.json')
pledges_resources = getJSON('data/scraped_rebus_pledges.json')
federations_resources = getJSON('data/scraped_rebus_federations.json')
benchmarks_resources = getJSON('data/scraped_cric_pandaqueue.json')
datadisk_info = getJSON('data/scraped_grafana_datadisk.json')

client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)

points_list = []

reader = mysql.connector.connect(user='monit', password=password, host='dbod-sql-graf.cern.ch', port=5501 ,database='monit_jobs')
read_cursor = reader.cursor()

logger.info('Getting existing data.')
read_cursor.execute("select panda_queue, resource, prod_source, avg1h_running_jobs, avg6h_running_jobs, avg12h_running_jobs, avg24h_running_jobs, avg7d_running_jobs, avg30d_running_jobs from jobs")

# Explicitly set timestamp in InfluxDB point. Avoids having multiple entries per 10 minute interval (can happen sometimes with acron)
epoch = datetime.utcfromtimestamp(0)
def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

current_time = datetime.utcnow().replace(microsecond=0,second=0,minute=0)
unix = int(unix_time_nanos(current_time))

for (panda_queue, resource, prod_source, avg1h_running_jobs, avg6h_running_jobs, avg12h_running_jobs, avg24h_running_jobs, avg7d_running_jobs, avg30d_running_jobs) in read_cursor:
    try:
        nickname = panda_resources[panda_queue] #do the mapping to actual panda queue nicknames
    except:
        logger.warning("Does not exist: queue: %s    Resource: %s" %(panda_queue, resource))
        continue

    logger.debug("Queue: %s    Resource: %s" %(panda_queue, resource))


    # simple hack to protect against duplicate entries
    # each site-core combination will have its unique **hash**
    m = hashlib.md5()
    m.update(str(panda_queue) + str(prod_source) + str(resource) + "running")
    time = unix + int(str(int(m.hexdigest(), 16))[0:9])

    if not panda_queue in panda_resources:
        print("ERROR  -  Site %s not in panda resources"%panda_queue)
        continue

    if not panda_queue in panda_queues:
        print("ERROR  -  Queue %s not in panda queues"%panda_queue)
        continue

    atlas_site = panda_queues.get(panda_queue,{}).get("atlas_site","None")
    type = panda_queues.get(panda_queue,{}).get("type","None")
    cloud = panda_queues.get(panda_queue,{}).get("cloud","None")
    site_state = panda_queues.get(panda_queue,{}).get("status","None")
    tier = panda_queues.get(panda_queue,{}).get("tier","None")

    #Resource factor
    if "MCORE" in resource:
        if panda_queues[panda_queue]["corecount"]:
            resource_factor = float(panda_queues[panda_queue]["corecount"])
        else:
            resource_factor = 8.0
    else:
        resource_factor = 1.0

    ddm_names = panda_queues.get(panda_queue,{}).get("astorages",{}).get("read_lan",[])
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

    tags = {
        "atlas_site": atlas_site,
        "panda_queue" : panda_queue,
        "resource" : resource,
        "datadisk_name" : datadisk_name,
        "type" : type,
        "prod_source" : prod_source,
        "cloud" : cloud,
        "site_state" : site_state,
        "job_status" : "running",
        "tier" : tier,
    }

    #give some useful default values
    for key in tags:
        if tags[key] == "":
            tags[key] = "No value"

    json_body = {   "measurement": "datadisks",
                    "tags": tags,
                    "time" : time,
                    "fields" : {
                        "datadisk_files" : datadisk_files,
                        "datadisk_occupied_gb" : datadisk_size,
                        "avg1h_running_jobs":customfloat(avg1h_running_jobs),
                        "avg6h_running_jobs":customfloat(avg6h_running_jobs),
                        "avg12h_running_jobs":customfloat(avg12h_running_jobs),
                        "avg24h_running_jobs":customfloat(avg24h_running_jobs),
                        "avg7d_running_jobs":customfloat(avg7d_running_jobs),
                        "avg30d_running_jobs":customfloat(avg30d_running_jobs),
                        "resource_factor" : resource_factor
                    }
                }

    points_list.append(json_body)


client.write_points(points=points_list, retention_policy="1h",time_precision="n")
