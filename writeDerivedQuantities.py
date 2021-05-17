#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
from collections import defaultdict
import json,sys

import requests
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

import argparse
from datetime import datetime,timedelta
import time
import dateutil.parser
import hashlib
import ConfigParser

import logging
from commonHelpers.logger import logger
logger = logger.getChild("QMonit")

from influxdb import InfluxDBClient
import Client
import mysql.connector

parser = argparse.ArgumentParser(description="Derived quantities writer")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('--skipSubmit', action='store_true', help='do not upload to DB')
args = parser.parse_args()

if args.debug:
    logging.getLogger("QMonit").setLevel(logging.DEBUG)

epoch = datetime.utcfromtimestamp(0)

def unix_time_nanos(dt):
    return (dt - epoch).total_seconds() * 1e9

def get_average_jobs(time_intervals, values, index, debug=False, skipFirst=False):
    total_jobs = 0
    for value in values[:time_intervals]:
        total_jobs += value[index]
        if debug:
            logger.debug(value[index])

    mean = float(total_jobs) / len(values[:time_intervals])

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

def get_pq_from_mysql(cursor):
    cursor.execute('SELECT DISTINCT panda_queue, prod_source, resource FROM jobs;')
    return [(pq,prod_source,resource) for pq,prod_source,resource in cursor]

def get_derived_quantities(distinct_sets, series, series_30d, pqs_mysql):

    mysql_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    pqs_in_idb = []

    for rs in distinct_sets.keys():
        rs = rs[1] #rs is a tuple

        data = get_derived_quantities_for_keyset(rs, series, series_30d)
        pqs_in_idb.append((rs["panda_queue"],rs["prod_source"],rs["resource"]))

        if data is None:
            mysql_data[rs["panda_queue"]][rs["prod_source"]][rs["resource"]][rs["job_status"]] = None
            mysql_data[rs["panda_queue"]][rs["prod_source"]][rs["resource"]]["tags"] = None
        else:
            if not 'values' in data:
                mysql_data[rs["panda_queue"]][rs["prod_source"]][rs["resource"]][rs["job_status"]] = None
            else:
                mysql_data[rs["panda_queue"]][rs["prod_source"]][rs["resource"]][rs["job_status"]] = data['values']

            mysql_data[rs["panda_queue"]][rs["prod_source"]][rs["resource"]]["tags"] = data['tags']

    #now let's run again over the PQs from the MySQL db to make sure non-existant PQs are correctly handled
    missing_pqs = sorted(set(pqs_mysql) - set(pqs_in_idb))

    return mysql_data,missing_pqs

def get_derived_quantities_for_keyset(rs, series, series_30d):
    logger.debug('Queue: %s     Prod source: %s       Resource: %s      State: %s' %(rs["panda_queue"],rs["prod_source"],rs["resource"],rs["job_status"]))

    filtered_points = [p for p in series if p['tags']['panda_queue'] == rs['panda_queue'] and p['tags']['prod_source'] == rs['prod_source'] and p['tags']['resource'] == rs['resource'] and p['tags']['job_status'] == rs['job_status']]
    filtered_points_30d = [p for p in series_30d if p['tags']['panda_queue'] == rs['panda_queue'] and p['tags']['prod_source'] == rs['prod_source'] and p['tags']['resource'] == rs['resource'] and p['tags']['job_status'] == rs['job_status']]

    if len(filtered_points) == 0:
        logger.debug('Got no points for this 10m set of keys.')
        return None
    if len(filtered_points_30d) == 0:
        logger.debug('Got no points for this 1d set of keys.')
        return None
    elif len(filtered_points) > 1 or len(filtered_points_30d) > 1:
        logger.debug('Uhh, oh, got more than one point? This is weird! I will use the first one and hope this is what you meant to do.')
        # print(filtered_points[0])
        # print(filtered_points[1])

    filtered_points = filtered_points[0]
    filtered_points_30d = filtered_points_30d[0]

    values = filtered_points['values']
    values_30d = filtered_points_30d['values']
    tags = filtered_points['tags']
    columns = filtered_points['columns']
    columns_30d = filtered_points_30d['columns']

    values.reverse() #reverse in place, want to have latest points first
    values_30d.reverse() #reverse in place, want to have latest points first

    #get me the last (most recent) point, because this is the one I want to overwrite.
    latest_value = values[0]

    data = {}
    data['current'] = latest_value[columns.index('jobs')]
    data['avg1h'] = get_average_jobs(6, values, columns.index('jobs'))
    data['avg6h'] = get_average_jobs(36, values, columns.index('jobs'))
    data['avg12h'] = get_average_jobs(72, values, columns.index('jobs'))
    data['avg24h'] = get_average_jobs(144, values, columns.index('jobs'))
    data['avg7d'] = get_average_jobs(7, values_30d, columns_30d.index('jobs'), debug=args.debug,skipFirst=True)
    data['avg30d'] = get_average_jobs(30, values_30d, columns_30d.index('jobs'), skipFirst=True)

    # print(data)
    return {'tags':tags, 'values':data}

def get_list_to_upload(data):
    for panda_queue, d in data.iteritems():
        if panda_queue == 'RAL-LCG2_MCORE_TEMP':
            logger.warning('We have some weird value here')
        for prod_source, resources in d.iteritems():
            for resource, job_states in resources.iteritems():
                temp_data = {}
                for job_status, job_data in job_states.iteritems():
                    if job_data is None:
                        continue
                    if job_status == "tags":
                        tags = job_data
                    else:
                        temp_data[job_status+"_jobs"] = job_data['current']
                        if job_status in ['assigned', 'activated', 'failed', 'running']:
                            for average in ["avg1h","avg6h","avg12h","avg24h","avg7d","avg30d"]:
                                temp_data[average+"_"+job_status+"_jobs"] = job_data[average]

                if len(temp_data) == 0:
                    continue

                add_point = ('''INSERT INTO jobs (panda_queue,prod_source, resource) VALUES ("{panda_queue}", "{prod_source}", "{resource}") ON DUPLICATE KEY UPDATE '''.format(
                            panda_queue = panda_queue,
                            prod_source = prod_source,
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
    logger.info('Getting distinct key sets')
    client = InfluxDBClient('dbod-eschanet.cern.ch', 8080, username, password, "monit_jobs", True, False)
    rs_distinct_sets = client.query('''select panda_queue, prod_source, resource, job_status, jobs from "1h"."jobs" where time > now() - 30d and "prod_source" != '' group by panda_queue, prod_source, resource, job_status limit 1''')

    logger.info('Getting 10m data')
    rs_result_24h = client.query('''select * from "10m"."jobs" where time > now() - 24h and "prod_source" != '' group by panda_queue, prod_source, resource, job_status ''')
    logger.info('Got 10m data')
    raw_dict_24h = rs_result_24h.raw
    series_24h = raw_dict_24h['series']

    logger.info('Getting 1d data')
    rs_result_30d = client.query('''select * from "1d"."jobs" where time > now() - 30d and "prod_source" != '' group by panda_queue, prod_source, resource, job_status ''')
    logger.info('Got 1d data')
    raw_dict_30d = rs_result_30d.raw
    series_30d = raw_dict_30d['series']

    logger.info('Got data from InfluxDB.')
    logger.info('Constructing MySQL connector.')

    cnx = mysql.connector.connect(user='monit', password=password, host='dbod-sql-graf.cern.ch', port=5501 ,database='monit_jobs')
    cursor = cnx.cursor()
    selector = cnx.cursor()

    #in mysql there may still be unique pq-resource combinations that don't exist anymore
    pqs_mysql = get_pq_from_mysql(selector)

    logger.info('Building data.')
    data,missing_pqs = get_derived_quantities(rs_distinct_sets, series_24h, series_30d, pqs_mysql)

    for point in get_list_to_upload(data):
        if args.debug:
            print(point)
        if not args.skipSubmit:
            cursor.execute(point)

    for pq,prod_source,resource in missing_pqs:
        cursor.execute('DELETE FROM jobs WHERE panda_queue = "{panda_queue}" AND resource = "{resource}" AND prod_source = "{prod_source}"'.format(panda_queue=pq,resource=resource,prod_source=prod_source))

    if not args.skipSubmit:
        cnx.commit()
        cursor.close()
        cnx.close()


if __name__== "__main__":
    run()
