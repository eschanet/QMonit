#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
from datetime import datetime,timedelta
import hashlib
import ConfigParser

import logging
from commonHelpers.logger import logger
logger = logger.getChild("mephisto")

from influxdb import InfluxDBClient
import Client

config = ConfigParser.ConfigParser()
config.read("config.cfg")

password = config.get("credentials", "password")
username = config.get("credentials", "username")
database = config.get("credentials", "database")

baseURL = 'http://pandaserver.cern.ch:25080/server/panda'

url_cloudJobs  = baseURL + '/getJobStatistics'
url_siteJobs   = baseURL + '/getJobStatisticsPerSiteResource'
url_bamboo     = baseURL + '/getJobStatisticsForBamboo'

bigpandaURL = 'https://bigpanda.cern.ch/dash/production/?cloudview=world&json'

TIMEOUT = 20

with open('pandaqueue_scraped.json') as pandaqueue:
    panda_queues = json.load(pandaqueue)

with open('pandaqueue_actual_map.json') as pandaresource:
    panda_resources = json.load(pandaresource)

with open('sites_scraped.json') as siteresource:
    site_resources = json.load(siteresource)

with open('ddm_scraped.json') as ddmresource:
    ddm_resources = json.load(ddmresource)

with open('daods_datadisk.json') as datadisks:
    datadisk_info = json.load(datadisks)

err, siteResourceStats = Client.getJobStatisticsPerSiteResource()

client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)

points_list = []

# Explicitly set timestamp in InfluxDB point. Avoids having multiple entries per 10 minute interval (can happen sometimes with acron)
epoch = datetime.utcfromtimestamp(0)
def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

current_time = datetime.utcnow()
current_time = current_time - timedelta(minutes=current_time.minute % 10,
                             seconds=current_time.second,
                             microseconds=current_time.microsecond)

unix = int(unix_time_nanos(current_time))

for site, site_result in siteResourceStats.iteritems():

    for core, value in site_result.iteritems():

        for job_status in value.keys():

            # simple hack to protect against duplicate entries
            # each site-core combination will have its unique **hash**
            m = hashlib.md5()
            m.update(site + core + job_status)
            time = unix + int(str(int(m.hexdigest(), 16))[0:9])

            if not site in panda_resources:
                print("ERROR  -  Site %s not in panda resources"%site)
                continue

            queue = panda_resources[site]

            if not queue in panda_queues:
                print("ERROR  -  Queue %s not in panda queues"%queue)
                continue

            atlas_site = panda_queues.get(queue,{}).get("atlas_site","None")
            type = panda_queues.get(queue,{}).get("type","None")
            cloud = panda_queues.get(queue,{}).get("cloud","None")
            site_state = panda_queues.get(queue,{}).get("status","None")
            tier = panda_queues.get(queue,{}).get("tier","None")

            #Resource factor
            if "MCORE" in core:
                if panda_queues[queue]["corecount"]:
                    resource_factor = float(panda_queues[queue]["corecount"])
                else:
                    resource_factor = 8.0
            else:
                resource_factor = 1.0

            ddm_names = panda_queues.get(queue,{}).get("ddm","None").split(",")
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

            n_jobs = value[job_status]

            tags = {
                "atlas_site": atlas_site,
                "panda_queue" : site,
                "resource" : core,
                "datadisk_name" : datadisk_name,
                "type" : type,
                "cloud" : cloud,
                "site_state" : site_state,
                "job_status" : job_status,
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
                                "jobs" : n_jobs,
                                "resource_factor" : resource_factor
                            }
                        }

            points_list.append(json_body)


client.write_points(points=points_list, retention_policy="1h",time_precision="n")
