#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
from commonHelpers import fileHelpers as fh


def run():

    # Get json from rebus
    url = "https://wlcg-rebus.cern.ch/apps/pledges/resources/2019/all/json"
    pledge_info = fh.get_json_from_url(url)

    json_info = {}
    for d in pledge_info:
        json_info[d["Federation"]] = d
    saved = fh.save_json_to_file("federation_pledges_scraped.json",json_info)


if __name__== "__main__":
    run()
