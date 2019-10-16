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

urllib.disable_warnings(urllib.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('--kill-last', action='store_true', help='print debug messages')
parser.add_argument('-average', default='1h', help='How much time to average over')
parser.add_argument('-measurement', default='jobs', help='What measurement to average')
args = parser.parse_args()

if args.debug:
    logging.getLogger("mephisto").setLevel(logging.DEBUG)

# Explicitly set timestamp in InfluxDB point. Avoids having multiple entries per 10 minute interval (can happen sometimes with acron)
epoch = datetime.utcfromtimestamp(0)
def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

if args.average == '1h':
    current_time = datetime.utcnow().replace(microsecond=0,second=0,minute=0)
else:
    current_time = datetime.utcnow().replace(microsecond=0,second=0,minute=0,hour=0)
unix = int(unix_time_nanos(current_time))

def get_sum(time_intervals, values, index):

    total_jobs = 0

    # create list first
    to_sum = [value[index] for value in values[:time_intervals] if not value[index] is None]
    return round(float(sum(to_sum)),2)


def get_average(time_intervals, values, index):

    total_jobs = 0

    # create list first
    to_sum = [value[index] for value in values[:time_intervals] if not value[index] is None]
    total_jobs = sum(to_sum)

    try:
        mean = float(total_jobs) / len(to_sum)
    except:
        logger.warning('Got unexpected averaged number, returning 0.0')
        return 0.0

    if isinstance(mean, float):
        return round(mean,2)
    else:
        logger.warning('Got unexpected averaged number, returning 0.0')
        return 0.0

def run():

    config = ConfigParser.ConfigParser()
    config.read("config.cfg")

    password = config.get("credentials", "password")
    username = config.get("credentials", "username")
    database = config.get("credentials", "database")

    logger.info('Constructing InfluxDB queries.')

    if args.average == '1h':
        retention = '10m'
        delta = '2h'
        time_units = 6
    elif args.average == '1d':
        retention = '1h'
        delta = '2d'
        time_units = 24
    else:
        return 0

    client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)
    rs_distinct_sets = client.query('''select * from "{}"."jobs" group by panda_queue, resource, job_status limit 1'''.format(retention))

    rs_result = client.query('''select * from "{}"."jobs" where time > now() - {} group by panda_queue, resource, job_status '''.format(retention,delta))
    raw_dict = rs_result.raw
    series = raw_dict['series']

    logger.info('Got data from InfluxDB.')
    logger.info('Averaging now.')


    # uploader = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "test", True, False)

    points_list = []
    for rs in rs_distinct_sets.keys():
        rs = rs[1] #rs is a tuple
        logger.debug(rs)

        filtered_points = [p for p in series if p['tags']['panda_queue'] == rs['panda_queue'] and p['tags']['resource'] == rs['resource'] and p['tags']['job_status'] == rs['job_status']]

        if len(filtered_points) == 0:
            logger.debug('Got no points for this set of keys.')
            continue

        filtered_points = filtered_points[0]

        values = filtered_points['values']
        tags = filtered_points['tags']
        columns = filtered_points['columns']

        #reverse in place, want to have latest points first
        values.reverse()

        #get me the last (most recent) point, because this is the one I want to overwrite.
        latest_value = values[0]

        # get averaged values
        if tags['job_status'] in ['failed','finished','cancelled','closed']:
            averaged_jobs = get_sum(time_units, values, columns.index('jobs'))
        else:
            averaged_jobs = get_average(time_units, values, columns.index('jobs'))
        # averaged_jobs = get_average(time_units, values, columns.index('jobs'))
        averaged_cpu = get_average(time_units, values, columns.index('resource_factor'))
        averaged_corepower = get_average(time_units, values, columns.index('corepower'))
        averaged_HS06_benchmark = get_average(time_units, values, columns.index('HS06_benchmark'))
        averaged_HS06_pledge = get_average(time_units, values, columns.index('federation_HS06_pledge'))

        #construct rest of the data dict
        data = dict(zip(columns, latest_value))

        time = data['time'].replace('T', ' ').replace('Z','')

        if args.average == '1h':
            hash = time.split('.')[-1].ljust(9, '0')
        else:
            #got no hashes in 1h aggregate data yet
            m = hashlib.md5()
            m.update(tags['panda_queue'] + tags['resource'] + tags['job_status'])
            hash = str(int(m.hexdigest(), 16))[0:9]

        time = unix + int(hash)

        data.update(tags)
        data.pop('time', None)
        data.pop('jobs', None)
        data.pop('resource_factor', None)
        data.pop('corepower', None)
        data.pop('HS06_benchmark', None)
        data.pop('federation_HS06_pledge', None)

        json_body = {   "measurement": "jobs",
                        "tags": data,
                        "time" : time,
                        "fields" : {
                            "jobs" : averaged_jobs,
                            "resource_factor" : averaged_cpu,
                            "corepower" : averaged_corepower,
                            "HS06_benchmark" : averaged_HS06_benchmark,
                            "federation_HS06_pledge" : averaged_HS06_pledge
                        }
                    }

        #sometimes I fuck up and then I want to kill the last measurement...
        if args.kill_last:
            for key,value in json_body['fields'].iteritems():
                json_body['fields'][key] = 0.0

        points_list.append(json_body)

    client.write_points(points=points_list, time_precision="n", retention_policy=args.average)

if __name__== "__main__":
    run()
