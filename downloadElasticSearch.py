#!/usr/bin/python

from __future__ import print_function

import urllib
import json, os
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
                            "size": 1,
                            "sort": [{"timestamp": "desc"}]
                        }
                    }
                },
                filter_path=[""])

    for hit in res['hits']['hits']:
        # pprint(hit)
        pq = hit.get('_source',{}).get('metadata',{}).get('PanDAQueue',None)
        if pq:
            latest_list = hit.get('inner_hits',{}).get('most_recent',{}).get('hits',{}).get('hits',[])
            if len(latest_list) > 0:
                results[pq] = latest_list[0].get('_source',{}).get('profiles',{}).get('fastBmk',{}).get('value',0.0)

    # pprint(results)
    saved = fh.save_json_to_file("benchmarks_elasticsearch_scraped.json",results)
    # saved = fh.save_json_to_file("test.json",results)


if __name__== "__main__":
    run()
