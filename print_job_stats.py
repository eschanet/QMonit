#!/usr/bin/python3.4

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
from datetime import datetime

# from influxdb import InfluxDBClient
import Client

#-------------------------------------------------------------------------------
baseURL = 'http://pandaserver.cern.ch:25080/server/panda'

url_cloudJobs  = baseURL + '/getJobStatistics'
url_siteJobs   = baseURL + '/getJobStatisticsPerSiteResource'
url_bamboo     = baseURL + '/getJobStatisticsForBamboo'

bigpandaURL = 'https://bigpanda.cern.ch/dash/production/?cloudview=world&json'

TIMEOUT = 20

with open('pandaqueue.json') as pandaqueue:
    panda_queues = json.load(pandaqueue)

with open('pandaresource.json') as pandaresource:
    panda_resources = json.load(pandaresource)
#-------------------------------------------------------------------------------
err, siteResourceStats = Client.getJobStatisticsPerSiteResource()

# pprint(siteResourceStats0)

# client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, 'admin', 'BachEscherGoedel', 'prod', True, False)

# pprint(siteResourceStats0['TRIUMF_DOCKER_UCORE'])

points_list = []

for site, site_result in siteResourceStats.iteritems():

    for core, value in site_result.iteritems():
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # print(site)
        #
        # pprint(value)

        if site in panda_resources:
            queue = panda_resources[site]

            atlas_site = panda_queues[queue]["atlas_site"]
            type = panda_queues[queue]["type"]
            cloud = panda_queues[queue]["cloud"]
            state = panda_queues[queue]["state"]

            json_body = {   "measurement": "jobs",
                            "tags": {
                                "atlas_site": atlas_site,
                                "panda_queue" : site,
                                "resource" : core,
                                "type" : type,
                                "cloud" : cloud,
                                "state" : state
                            },
                            "time" : current_time,
                            "fields" : value
                        }

            points_list.append(json_body)
        else:
            print("ERROR  -  Site %s not in panda resources"%site)
            print(site)

            pprint(value)

        # pprint(json_body)

# print("Number of points to be uploaded")
# pprint(points_list)

#client.write_points(points=points_list, time_precision="ms")
