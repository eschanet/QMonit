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

#let's load some of the information that has been scraped previously
with open('pandaqueue_scraped.json') as pandaqueue:
    panda_queues = json.load(pandaqueue)
with open('pandaqueue_actual_map.json') as pandaresource:
    panda_resources = json.load(pandaresource)
with open('sites_scraped.json') as siteresource:
    site_resources = json.load(siteresource)
with open('ddm_scraped.json') as ddmresource:
    ddm_resources = json.load(ddmresource)
with open('federation_pledges_scraped.json') as pledgesresource:
    pledges_resources = json.load(pledgesresource)
with open('federations_scraped.json') as federationsresource:
    federations_resource = json.load(federationsresource)
with open('benchmarks_elasticsearch_scraped.json') as benchmarksresource:
    benchmarks_resource = json.load(benchmarksresource)

#get the actual job numbers from panda
err, siteResourceStats = Client.getJobStatisticsPerSiteResource()

#idb client instance for uploading data later on
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
            federation = federations_resource.get(atlas_site,{}).get("Federation","None")
            pledge = ""
            for pledge_types,pledge_units in zip(pledges_resources.get(federation,{}),pledges_resources.get(federation,{})):
                pledge_type = pledge_types.get("PledgeType","None")
                pledge_unit = pledge_units.get("PledgeUnit","None")
                pledge += "%s (%s);" % (pledge_type, pledge_unit)
            if len(pledge)>1: #not sure if needed? is below safe for empty strings?
                pledge = pledge[:-1]

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
            benchmark_corepower = float(benchmarks_resource.get(queue,{}).get("avg_value",0.0))

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
                            }
                        }

            points_list.append(json_body)


client.write_points(points=points_list, time_precision="n")
