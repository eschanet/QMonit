#!/usr/bin/python

from __future__ import print_function

import argparse
from scrapers import scraper_classes as scrapers
from maps import PQ_names_map as pq_map


import logging
from commonHelpers.logger import logger

logger = logger.getChild("mephisto")

parser = argparse.ArgumentParser(description="Run a set of JSON/web scrapers")
parser.add_argument('--debug', action='store_true', help='print debug messages')
parser.add_argument('-interval', default='10m', help='Defines which scrapers are being run')
args = parser.parse_args()

def run():

    # Each time the scrapers are run, we update the PQ map
    pqs = pq_map.PQ_names_map(file="map_PQ_names.json")
    pqs.update(ifile="scraped_agis_pandaqueue.json",ofile="map_PQ_names.json",key="panda_resource")

    if args.interval == '10m':
        # Now run all the scrapers that should run in 10min intervals
        # First panda queue names map
        agis = scrapers.AGIS()
        raw_data = agis.download(url="http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all")
        json_data = agis.convert(data=raw_data,pq_field="panda_resource")
        saved = agis.save(file="scraped_agis_pandaqueue.json",data=json_data)

    elif args.interval == '1h':
        # Run all the scrapers that only need to be run once per hour (because they don't change too often)
        print("hourly scrapers")
    else:
        # Nothing to do otherwise
        print("Dropping out")


if __name__== "__main__":
    run()
