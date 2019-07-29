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
                    "query" : {
                        "range" : {
                            "timestamp" : {
                                "gte" : "now-1000d/d",
                                "lt" :  "now/d"
                            }
                        }
                    },
                    "size": 10000,
                    "aggs": {
                        "pq_agg": {
                            "terms": {
                                "field": "metadata.PanDAQueue"
                            },
                            "aggs": {
                                "group_docs": {
                                    "top_hits": {
                                        "size": 1,
                                        "sort": [
                                        {
                                            "timestamp": {
                                                "order": "desc"
                                            }
                                        }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                },
                filter_path=[""])

    for hit in res['hits']['hits']:
        pq = hit.get('_source',{}).get('metadata',{}).get('PanDAQueue',None)
        if pq:
            results[pq] = hit.get('_source',{}).get('profiles',{}).get('fastBmk',{}).get('value',0.0)

    saved = fh.save_json_to_file("benchmarks_elasticsearch_scraped.json",results)


if __name__== "__main__":
    run()
