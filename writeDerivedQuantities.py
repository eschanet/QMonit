#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
from collections import defaultdict
import json,sys
import requests
import argparse
from datetime import datetime,timedelta
import time
import dateutil.parser
import hashlib
import ConfigParser

import logging
from commonHelpers.logger import logger
logger = logger.getChild("mephisto")

from influxdb import InfluxDBClient
import Client
import mysql.connector

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument('--debug', action='store_true', help='print debug messages')
args = parser.parse_args()

if args.debug:
    logging.getLogger("mephisto").setLevel(logging.DEBUG)

epoch = datetime.utcfromtimestamp(0)

def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

def get_average_jobs(time_intervals, values, index, debug=False):
    total_jobs = 0
    for value in values[:time_intervals]:
        total_jobs += value[index]

    mean = float(total_jobs) / time_intervals
    if isinstance(mean, float):
        return round(mean,2)
    else:
        logger.warning('Got unexpected mean job number, returning 0.0')
        return 0.0

def construct_valdict_from_lists(columns,values):
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
        return 0

def get_derived_quantities(distinct_sets, series):

    mysql_data = defaultdict(lambda: defaultdict(dict))

    for rs in distinct_sets.keys():
        rs = rs[1] #rs is a tuple

        if args.debug and not (rs['panda_queue'] == 'AGLT2_HOSPITAL' and rs['resource'] == 'MCORE'):
            continue

        data = get_derived_quantities_for_keyset(rs, series)

        if data is None:
            mysql_data[rs["panda_queue"]][rs["resource"]][rs["job_status"]] = None
            mysql_data[rs["panda_queue"]][rs["resource"]]["tags"] = None
        else:
            if not 'values' in data:
                mysql_data[rs["panda_queue"]][rs["resource"]][rs["job_status"]] = None
            else:
                mysql_data[rs["panda_queue"]][rs["resource"]][rs["job_status"]] = data['values']

            mysql_data[rs["panda_queue"]][rs["resource"]]["tags"] = data['tags']

    return mysql_data

def get_derived_quantities_for_keyset(rs, series):
    logger.debug('Queue: %s     Resource: %s      State: %s' %(rs["panda_queue"],rs["resource"],rs["job_status"]))

    filtered_points = [p for p in series if p['tags']['panda_queue'] == rs['panda_queue'] and p['tags']['resource'] == rs['resource'] and p['tags']['job_status'] == rs['job_status']]

    if len(filtered_points) == 0:
        logger.debug('Got no points for this set of keys.')
        return None
    elif len(filtered_points) > 1:
        logger.debug('Uhh, oh, got more than one point? This is weird! I will use the first one and hope this is what you meant to do.')

    filtered_points = filtered_points[0]

    values = filtered_points['values']
    tags = filtered_points['tags']
    columns = filtered_points['columns']

    values.reverse() #reverse in place, want to have latest points first

    #get me the last (most recent) point, because this is the one I want to overwrite.
    latest_value = values[0]

    data = {}
    data['current'] = latest_value[columns.index('jobs')]
    data['avg1h'] = get_average_jobs(6, values, columns.index('jobs'))
    data['avg6h'] = get_average_jobs(36, values, columns.index('jobs'))
    data['avg12h'] = get_average_jobs(72, values, columns.index('jobs'))
    data['avg24h'] = get_average_jobs(144, values, columns.index('jobs'))

    return {'tags':tags, 'values':data}

def get_list_to_upload(data):
    for panda_queue, d in data.iteritems():
        for resource, job_states in d.iteritems():
            temp_data = {}
            for job_status, job_data in job_states.iteritems():
                if job_data is None:
                    continue
                if job_status == "tags":
                    tags = job_data
                else:
                    temp_data[job_status+"_jobs"] = job_data['current']
                    if job_status in ['assigned', 'activated', 'failed', 'running']:
                        temp_data["avg1h_"+job_status+"_jobs"] = job_data['avg1h']
                        temp_data["avg6h_"+job_status+"_jobs"] = job_data['avg6h']
                        temp_data["avg12h_"+job_status+"_jobs"] = job_data['avg12h']
                        temp_data["avg24h_"+job_status+"_jobs"] = job_data['avg24h']

            if len(temp_data) == 0:
                continue

            add_point = ('''INSERT INTO jobs (atlas_site, panda_queue, resource) VALUES ("{atlas_site}", "{panda_queue}", "{resource}") ON DUPLICATE KEY UPDATE '''.format(
                        atlas_site = tags["atlas_site"],
                        panda_queue = panda_queue,
                        resource = resource))

            for field, value in temp_data.iteritems():
                add_point += '''{field}={value}, '''.format(field=field, value=value)

            add_point = add_point[:-2] + ''';'''

            logger.debug(add_point)

            yield add_point

def run():

    config = ConfigParser.ConfigParser()
    config.read("config.cfg")

    password = config.get("credentials", "password")
    username = config.get("credentials", "username")
    database = config.get("credentials", "database")

    logger.info('Constructing InfluxDB queries.')

    client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)
    rs_distinct_sets = client.query('''select * from "10m"."jobs" group by panda_queue, resource, job_status limit 1''')
    rs_result = client.query('''select * from "10m"."jobs" where time > now() - 24h group by * ''')

    raw_dict = rs_result.raw
    series = raw_dict['series']

    logger.info('Got data from InfluxDB.')
    logger.info('Constructing MySQL connector.')

    cnx = mysql.connector.connect(user='monit', password=password, host='dbod-sql-graf.cern.ch', port=5501 ,database='monit_jobs')
    cursor = cnx.cursor()

    logger.info('Building data.')

    data = get_derived_quantities(rs_distinct_sets, series)

    for point in get_list_to_upload(data):
        cursor.execute(point)

    # client.write_points(points=points_list, time_precision="n")

    cnx.commit()
    cursor.close()
    cnx.close()


if __name__== "__main__":
    run()
