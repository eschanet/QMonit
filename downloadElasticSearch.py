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
                        "match_all" : {},
                        # "bool" : {
                        #     "must" : {
                        #         "match" : {
                        #             "metadata.PanDAQueue" : "ANALY_LRZ",
                        #             "metadata.PanDAQueue" : "ANALY_CPPM"
                        #
                        #         }
                        #     }
                        # }
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
            if (date > (datetime.datetime.now() - datetime.timedelta(days=2))) and len(values) > 25:
                continue
            if (date < (datetime.datetime.now() - datetime.timedelta(days=7))) or len(values) < 5:
                #we are within the last 7 days and haven't got 5 days outside of 7d yet
                values.append(d)

        to_average = [i.get('_source',{}).get('profiles',{}).get('fastBmk',{}).get('value',0.0) for i in values]
        results[pq] = float(sum(to_average))/len(to_average)
        # print(len(to_average))

    # pprint(results)
    saved = fh.save_json_to_file("benchmarks_elasticsearch_scraped.json",results)
    # saved = fh.save_json_to_file("test.json",results)


if __name__== "__main__":
    run()
