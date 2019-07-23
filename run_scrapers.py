#!/usr/bin/python

from __future__ import print_function

import argparse
from scrapers import scraper_classes as scrapers


import logging
from commonHelpers.logger import logger

logger = logger.getChild("mephisto")

parser = argparse.ArgumentParser(description="Run a set of JSON/web scrapers")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('-interval', default='10m', help='Defines which scrapers are being run')
args = parser.parse_args()

def run():

    if args.interval == '10m':
        # Now run all the scrapers that should run in 10min intervals
        agis = scrapers.AGIS()
        raw_data = agis.download(url="http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all")
        json_data = agis.convert(data=raw_data,pq_field="panda_resource")
        saved = agis.save(file="PQ_names_map.json",data=json_data)

    elif args.interval == '1h':
        # Run all the scrapers that only need to be run once per hour (because they don't change too often)

    else:
        # Nothing to do otherwise



if __name__== "__main__":
    run()
