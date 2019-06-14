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

    # Also need to get DDM information
    url = "http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&"
    ddm_info = get_json_from_url(url)

    json_info = {}
    for d in ddm_info:
        json_info[d["site"]] = d
    saved = save_json_to_file("ddm_scraped.json",json_info)



if __name__== "__main__":
    run()