#!/usr/bin/python

from __future__ import print_function

import argparse

from scrapers.agis import AGIS
from scrapers.rebus import REBUS

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
    pqs = pq_map.PQ_names_map(file="data/map_PQ_names.json")
    pqs.update(ifile="data/scraped_agis_pandaqueue.json",ofile="data/map_PQ_names.json",key="panda_resource")

    if args.interval == '10m':
        # Now run all the scrapers that should run in 10min intervals
        # First the PQ AGIS information
        agis = AGIS()
        raw_data = agis.download(url="http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all")
        json_data = agis.convert(data=raw_data,sort_field="panda_resource")
        if agis.save(file="data/scraped_agis_pandaqueue.json",data=json_data):
            logger.info("Scraped PQ AGIS")
        else:
            logger.error("Problem scraping PQ AGIS")

    elif args.interval == '1h':
        # Run all the scrapers that only need to be run once per hour (because they don't change too often)

        # Next the ATLAS sites AGIS information
        agis = AGIS()
        raw_data = agis.download(url="http://atlas-agis-api.cern.ch/request/site/query/list/?json&")
        json_data = agis.convert(data=raw_data,sort_field="name")
        if agis.save(file="data/scraped_agis_sites.json",data=json_data):
            logger.info("Scraped sites AGIS")
        else:
            logger.error("Problem scraping sites AGIS")

        # Now the DDM info from AGIS
        raw_data = agis.download(url="http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&")
        json_data = agis.convert(data=raw_data,sort_field="site")
        if agis.save(file="data/scraped_agis_ddm.json",data=json_data):
            logger.info("Scraped DDM AGIS")
        else:
            logger.error("Problem scraping DDM AGIS")

        # Next up is REBUS, start with the actual federation map
        rebus = REBUS()
        raw_data = rebus.download(url="https://wlcg-rebus.cern.ch/apps/topology/all/json")
        json_data = rebus.convert(data=raw_data,sort_field="Site")
        if rebus.save(file="data/scraped_rebus_federations.json",data=json_data):
            logger.info("Scraped federations REBUS")
        else:
            logger.error("Problem scraping federations REBUS")

        raw_data = rebus.download(url="https://wlcg-rebus.cern.ch/apps/pledges/resources/2019/all/json")
        json_data = rebus.convert(data=raw_data,sort_field="Federation")
        if rebus.save(file="data/scraped_rebus_pledges.json",data=json_data):
            logger.info("Scraped pledges REBUS")
        else:
            logger.error("Problem scraping pledges REBUS")

    else:
        # Nothing to do otherwise
        print("Dropping out")


if __name__== "__main__":
    run()
