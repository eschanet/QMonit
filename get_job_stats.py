#!/usr/bin/python3.4

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
from datetime import datetime

from influxdb import InfluxDBClient
import Client

#-------------------------------------------------------------------------------
baseURL = 'http://pandaserver.cern.ch:25080/server/panda'

url_cloudJobs  = baseURL + '/getJobStatistics'
url_siteJobs   = baseURL + '/getJobStatisticsPerSiteResource'
url_bamboo     = baseURL + '/getJobStatisticsForBamboo'

bigpandaURL = 'https://bigpanda.cern.ch/dash/production/?cloudview=world&json'

TIMEOUT = 20

#-------------------------------------------------------------------------------
err, siteResourceStats0 = Client.getJobStatisticsPerSiteResource()

# pprint(siteResourceStats0)

client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, 'admin', 'BachEscherGoedel', 'prod', True, False)

# pprint(siteResourceStats0['TRIUMF_DOCKER_UCORE'])

points_list = []

for site, site_result in siteResourceStats0.iteritems():

    for core, value in site_result.iteritems():
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        json_body = {   "measurement": "jobs",
                        "tags": {
                            "site": site,
                            "resource" : core,
                        },
                        "time" : current_time,
                        "fields" : value
                    }

        points_list.append(json_body)

        # print(current_time)
        # pprint(json_body)

# print("Number of points to be uploaded")
# print(len(points_list))

#client.write_points(points=points_list, time_precision="ms")
