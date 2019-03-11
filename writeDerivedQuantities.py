#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
from datetime import datetime,timedelta
import time
import dateutil.parser
import hashlib
import ConfigParser

from influxdb import InfluxDBClient
import Client

epoch = datetime.utcfromtimestamp(0)
def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

def getAverageJobs(time_intervals, values, index):
    total_jobs = 0
    for value in values[:time_intervals]:
        total_jobs += value[index]
    return float(total_jobs) / time_intervals

def constructValuesDictFromLists(columns,values):
    d = dict(zip(columns, values))
    d.pop("time", None)
    return d

def correctTypes(fields):
    if not isinstance(fields['resource_factor'], float):
        fields['resource_factor'] = float(fields['resource_factor'])
    return fields

def getUnixFromTimeStamp(time):
    time = time.replace('T', ' ').replace('Z','')
    hash = time[-9:]
    _time = time[:-10]
    try:
        dt = datetime.strptime(_time, '%Y-%m-%d %H:%M:%S')
        return int(unix_time_nanos(dt)) + int(hash)
    except ValueError:
        # print("Oh, I can't do this!")
        return 0

config = ConfigParser.ConfigParser()
config.read("config.cfg")

password = config.get("credentials", "password")
username = config.get("credentials", "username")
database = config.get("credentials", "database")

client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)

rs_distinct_sets = client.query('''select * from "10m"."jobs" group by panda_queue, resource limit 1''')
rs_result = client.query('''select * from "10m"."jobs" where "job_status" = 'running' and time > now() - 24h group by * ''')

#for 1w ratio, I don't care if I get a new value only once per hour...

points_list = []

# print(rs_result.keys())

for rs in rs_distinct_sets.keys():
    rs = rs[1] #rs is a tuple

    # points = list(rs_result.get_points(measurement='jobs', tags={'panda_queue': rs['panda_queue'], 'resource': rs['resource'] }))
    raw_dict = rs_result.raw
    series = raw_dict['series']

    filtered_points = [p for p in series if p['tags']['panda_queue'] == rs['panda_queue'] and p['tags']['resource'] == rs['resource']]

    if len(filtered_points) == 0:
        continue
    elif len(filtered_points) > 1:
        print('Uhh, oh, got more than one point? This is weird! I will use the first one and hope this is what you meant to do.')

    filtered_points = filtered_points[0]

    values = filtered_points['values']
    tags = filtered_points['tags']
    columns = filtered_points['columns']

    values.reverse() #reverse in place, want to have latest points first

    #get me the last (most recent) point, because this is the one I want to overwrite.
    latest_value = values[0]

    fields = constructValuesDictFromLists(columns,latest_value)

    fields[u'avg_1h'] = getAverageJobs(6, values, columns.index('jobs'))
    fields[u'avg_6h'] = getAverageJobs(36, values, columns.index('jobs'))
    fields[u'avg_12h'] = getAverageJobs(72, values, columns.index('jobs'))
    fields[u'avg_24h'] = getAverageJobs(144, values, columns.index('jobs'))

    fields = correctTypes(fields)
    time = getUnixFromTimeStamp(latest_value[columns.index('time')])

    if time == 0:
        # print("Can't do this: ")
        # pprint(tags)
        continue

    json_body = {   "measurement": u'jobs',
                    "tags": tags,
                    "time" : time,
                    "fields" : fields
                }

    # if tags['panda_queue'] == 'LRZ-LMU_UCORE':
    #     debug = json_body

    points_list.append(json_body)

# pprint(debug)

client.write_points(points=points_list, time_precision="n")
