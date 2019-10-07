#!/usr/bin/python

from __future__ import print_function

import urllib
import json, os
import datetime
from pprint import pprint
from commonHelpers import fileHelpers as fh

from elasticsearch6 import Elasticsearch

import ConfigParser

#do some configurations
config = ConfigParser.ConfigParser()
config.read("config.cfg")

#get credentials
password = config.get("credentials_elasticsearch", "password")
username = config.get("credentials_elasticsearch", "username")
host = config.get("credentials_elasticsearch", "host")

def run():

    results = {}

    es = Elasticsearch(
                [{'host': host, 'port': 9200}],
                http_auth=(username, password)
            )

    res = es.search(
                index="benchmarks-*",
                body={
                    "size" : 10000,
                    "query" : {
                        # "match_all" : {},
                        "bool" : {
                            "must" : {
                                "bool": {
                                    "should" : [
                                        { "match" : {"metadata.PanDAQueue" : "LRZ-LMU_UCORE"}},
                                        { "match" : {"metadata.PanDAQueue" : "ANALY_LRZ"}}
                                    ],
                                    "minimum_should_match" : 1
                                }
                            }
                        }
                    },
                    "collapse": {
                        "field": "metadata.PanDAQueue",
                        "inner_hits": {
                            "name": "most_recent",
                            "size": 50,
                            "sort": [{"timestamp": "desc"}]
                        }
                    }
                },
                filter_path=[""])

    saved = fh.save_json_to_file("benchmarks_elasticsearch_raw.json",res)


    for hit in res['hits']['hits']:
        # pprint(hit)

        # get the PQ
        pq = hit.get('_source',{}).get('metadata',{}).get('PanDAQueue',None)
        if not pq:
            continue

        # get the list of all benchmark results
        latest_list = hit.get('inner_hits',{}).get('most_recent',{}).get('hits',{}).get('hits',[])
        if len(latest_list)==0:
            continue

        # get the average of the latest benchmark results.
        # Only results not older than 7d, and a maximum of 50 results (whichever value is hit first).
        # If we have no values more recent than 7d, simply use the last available one (that PQ is probably not online anymore anyway)
        values=[]
        for d in latest_list:
            date = datetime.datetime.strptime( d.get('_source',{}).get('timestamp',""), '%Y-%m-%dT%H:%M:%SZ')
            two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
            seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)

            if date > two_days_ago:
                # we are within the last two days, so we take all the measurements we can get!
                values.append(d)
            elif (date < two_days_ago) and (date > seven_days_ago):
                # we are between 2 and 7 days ago, so take only values if we don't have 25 values already
                if len(values) < 30 :
                    values.append(d)
            elif date < seven_days_ago:
                # we are further away than 7 days, so take a maximum of 5 values from here if we don't have 5 yet
                if len(values) < 10:
                    values.append(d)

        to_average = [i.get('_source',{}).get('profiles',{}).get('fastBmk',{}).get('value',0.0) for i in values]
        results[pq] = {"avg_value" : float(sum(to_average))/len(to_average), "measurements" : len(to_average) }
        # print(len(to_average))

    pprint(results)
    saved = fh.save_json_to_file("benchmarks_elasticsearch_scraped.json",results)
    # saved = fh.save_json_to_file("test.json",results)


if __name__== "__main__":
    run()
