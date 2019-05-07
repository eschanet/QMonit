#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
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

            atlas_site = panda_queues[queue]["atlas_site"]
            type = panda_queues[queue]["type"]
            cloud = panda_queues[queue]["cloud"]
            site_state = panda_queues[queue]["status"]
            tier = panda_queues[queue]["tier"]
            pilot_manager = panda_queues[queue]["pilot_manager"]
            pilot_version = panda_queues[queue]["pilot_version"]
            harvester = panda_queues[queue]["harvester"]
            harvester_workflow = panda_queues[queue]["workflow"]

            #information about frontier
            frontier_list = site_resources.get(atlas_site, {}).get("fsconf", {}).get("frontier", [])
            if len(frontier_list) > 0:
                frontier = frontier_list[0]
            else:
                frontier = ''

            #information about nucleus
            data_policies = site_resources.get(atlas_site,{}).get("datapolicies",[])
            # if len(data_policies) > 0:
            #     frontier = frontier_list[0]
            # else:
            #     frontier = ''



            if "MCORE" in core:
                if panda_queues[queue]["corecount"]:
                    resource_factor = float(panda_queues[queue]["corecount"])
                else:
                    resource_factor = 8.0
            else:
                resource_factor = 1.0

            # if job_status == "running":
            #     n_jobs = value[job_status]*int(resource_factor)
            # else:
            n_jobs = value[job_status]

            json_body = {   "measurement": "jobs",
                            "tags": {
                                "atlas_site": atlas_site,
                                "panda_queue" : site,
                                "resource" : core,
                                "type" : type,
                                "cloud" : cloud,
                                "site_state" : site_state,
                                "job_status" : job_status,
                                "tier" : tier,
                                "pilot_manager" : pilot_manager,
                                "pilot_version" : pilot_version,
                                "frontier" : frontier,
                                "harvester" : harvester,
                                "workflow" : harvester_workflow,
                                # "fts_server" : fts_server,
                                # "data_policies" : data_policies,
                            },
                            "time" : time,
                            "fields" : {
                                "jobs" : n_jobs,
                                "resource_factor" : resource_factor
                            }
                        }

            points_list.append(json_body)


client.write_points(points=points_list, time_precision="n")
