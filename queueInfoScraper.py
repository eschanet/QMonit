#!/usr/bin/python

from __future__ import print_function

import urllib
import json
from pprint import pprint


def get_json_from_url(url):
    response = urllib.urlopen(url)
    data = json.load(response)
    return data

def save_json_to_file(filename,data):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
        return True
    except IOError:
        print("Got an error saving to file.")
        return False

def run():

    # First getting and saving general AGIS PanDA queue information
    url = "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all"
    queue_info = get_json_from_url(url)
    saved = save_json_to_file("pandaqueue_scraped.json",queue_info)

    #Creating a map between AGIS PanDA queue names and actual full PanDA queue names
    panda_resources = {}
    for queue,values in queue_info.iteritems():
        panda_resources[values["panda_resource"]] = queue

    saved = save_json_to_file("pandaqueue_actual_map.json",panda_resources)



if __name__== "__main__":
    run()
