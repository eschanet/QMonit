#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
from datetime import datetime,timedelta
import hashlib
import ConfigParser
import argparse

from influxdb import InfluxDBClient
import Client

parser = argparse.ArgumentParser(description="Write job stats")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('--skipSubmit', action='store_true', help='do not upload to DB')
args = parser.parse_args()


#do some configurations
config = ConfigParser.ConfigParser()
config.read("config.cfg")

#get credentials
password = config.get("credentials", "password")
username = config.get("credentials", "username")
database = config.get("credentials", "database")

#define my urls
baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
url_cloudJobs  = baseURL + '/getJobStatistics'
url_siteJobs   = baseURL + '/getJobStatisticsPerSiteResource'
url_bamboo     = baseURL + '/getJobStatisticsForBamboo'
bigpandaURL = 'https://bigpanda.cern.ch/dash/production/?cloudview=world&json'

def getJSON(file):
    with open(file) as f:
        return json.load(f)

panda_queues = getJSON('data/scraped_agis_pandaqueue.json')
panda_resources = getJSON('data/map_PQ_names.json')
site_resources = getJSON('data/scraped_agis_sites.json')
ddm_resources = getJSON('data/scraped_agis_ddm.json')
pledges_resources = getJSON('data/scraped_rebus_pledges.json')
federations_resources = getJSON('data/scraped_rebus_federations.json')
benchmarks_resources = getJSON('data/scraped_elasticsearch_benchmark.json')

#get the actual job numbers from panda
err, siteResourceStats = Client.getJobStatisticsPerSiteResource(10)

#idb client instance for uploading data later on
if not args.skipSubmit:
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

#unique data point is characterised by pqueue, resource type and job status
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

            #information taken from main AGIS json
            atlas_site = panda_queues.get(queue,{}).get("atlas_site","None")
            type = panda_queues.get(queue,{}).get("type","None")
            cloud = panda_queues.get(queue,{}).get("cloud","None")
            site_state = panda_queues.get(queue,{}).get("status","None")
            tier = panda_queues.get(queue,{}).get("tier","None")
            pilot_manager = panda_queues.get(queue,{}).get("pilot_manager","None")
            pilot_version = panda_queues.get(queue,{}).get("pilot_version","None")
            harvester = panda_queues.get(queue,{}).get("harvester","None")
            harvester_workflow = panda_queues.get(queue,{}).get("workflow","None")
            container_type = panda_queues.get(queue,{}).get("container_type","None")

            #information from wlcg rebus
            federation = federations_resources.get(atlas_site,{}).get("Federation","None")
            pledge = ""
            for pledges in pledges_resources.get(federation,[]):
                pledge_type = pledges.get("PledgeType","None")
                pledge_unit = pledges.get("PledgeUnit","None")
                pledge += "%s (%s);" % (pledge_type, pledge_unit)
                if pledge_unit == 'HEP-SPEC06':
                    federation_HS06_pledge = pledges.get("ATLAS",0)
            pledge = pledge[:-1] if len(pledge)>1 else "None" #wait a minute, is this safe for empty strings?

            #information about frontier
            frontier_list = site_resources.get(atlas_site, {}).get("fsconf", {}).get("frontier", [])
            if len(frontier_list) > 0:
                frontier = frontier_list[0]
                if len(frontier) > 0:
                    frontier = frontier[0]
                    frontier = frontier.split(':')[1].replace("//","")
            else:
                frontier = ''

            #information about nucleus
            data_policies = site_resources.get(atlas_site,{}).get("datapolicies",[])
            if "Nucleus" in data_policies:
                nucleus = atlas_site
            else:
                nucleus = 'None'

            # FTS server information
            fts_servers = ddm_resources.get(atlas_site,{}).get("servedrestfts",{}).get("MASTER",{})
            if len(fts_servers) > 0:
                fts_server = fts_servers[0].split(':')[1].replace("//","")
            else:
                fts_server = ''

            #Resource factor
            if "MCORE" in core:
                if panda_queues[queue]["corecount"]:
                    resource_factor = float(panda_queues[queue]["corecount"])
                else:
                    resource_factor = 8.0
            else:
                resource_factor = 1.0

            # Corepower
            corepower = float(panda_queues.get(queue,{}).get("corepower","1.0"))

            # Benchmarked HS06
            benchmark_corepower = float(benchmarks_resources.get(queue,{}).get("avg_value",0.0))

            n_jobs = value[job_status]

            tags = {
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
                "fts_server" : fts_server,
                "nucleus" : nucleus,
                "container_type": container_type,
                "federation" : federation,
                "pledge_type" : pledge,
            }

            #give some useful default values
            for key in tags:
                if tags[key] == "":
                    tags[key] = "No value"

            json_body = {   "measurement": "jobs",
                            "tags": tags,
                            "time" : time,
                            "fields" : {
                                "jobs" : n_jobs,
                                "resource_factor" : resource_factor,
                                "corepower" : corepower,
                                "HS06_benchmark" : benchmark_corepower,
                                "federation_HS06_pledge" : federation_HS06_pledge,
                            }
                        }

            points_list.append(json_body)

if not args.skipSubmit:
    client.write_points(points=points_list, time_precision="n")
