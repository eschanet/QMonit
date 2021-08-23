#!/usr/bin/python

from __future__ import print_function

import time
import argparse
import ConfigParser
import pprint

from scrapers.agis import AGIS  # EOL is near
from scrapers.rebus import REBUS  # EOL is near
from scrapers.cric import CRIC
from scrapers.grafana import Grafana
from scrapers.elasticsearch import ElasticSearch

from maps import PQ_names_map as pq_map

import logging
from commonHelpers.logger import logger
from commonHelpers import notifications

# do some configurations
config = ConfigParser.ConfigParser()
config.read("config.cfg")

logger = logger.getChild("mephisto")

parser = argparse.ArgumentParser(description="Run a set of JSON/web scrapers")
parser.add_argument("--debug", action="store_true", help="print debug messages")
parser.add_argument(
    "-interval", default="10m", help="Defines which scrapers are being run"
)
argparse = parser.parse_args()

if argparse.debug:
    logger.setLevel(logging.DEBUG)


def run():

    # Each time the scrapers are run, we update the PQ map
    pqs = pq_map.PQ_names_map(file="data/map_PQ_names.json")
    if not pqs.update(
        ifile="data/scraped_cric_pandaqueue.json",
        ofile="data/map_PQ_names.json",
        key="panda_resource",
    ):
        logger.warning("PQ map is not available")

    if argparse.interval == "10m":
        # Now run all the scrapers that should run in 10min intervals
        # First the PQ CRIC information
        cric = CRIC()
        raw_data = cric.download(
            url="https://atlas-cric.cern.ch/api/atlas/pandaqueue/query/?json"
        )
        json_data = cric.convert(data=raw_data, sort_field="panda_resource")
        if cric.save(file="data/scraped_cric_pandaqueue.json", data=json_data):
            logger.info("Scraped PQ CRIC")
        else:
            logger.error("Problem scraping PQ CRIC")

    elif argparse.interval == "1h":
        # Run all the scrapers that only need to be run once per hour (because they don't change too often)

        # Next the ATLAS sites CRIC information
        cric = CRIC()
        raw_data = cric.download(
            url="https://atlas-cric.cern.ch/api/atlas/site/query/?json"
        )
        json_data = cric.convert(data=raw_data, sort_field="name")
        if cric.save(file="data/scraped_cric_sites.json", data=json_data):
            logger.info("Scraped sites CRIC")
        else:
            logger.error("Problem scraping sites CRIC")

        # Now the DDM info from CRIC
        raw_data = cric.download(
            url="https://atlas-cric.cern.ch/api/atlas/ddmendpoint/query/?json"
        )
        json_data = cric.convert(data=raw_data, sort_field="site")
        if cric.save(file="data/scraped_cric_ddm.json", data=json_data):
            logger.info("Scraped DDM CRIC")
        else:
            logger.error("Problem scraping DDM CRIC")

        # Next up is REBUS, start with the actual federation map
        rebus = REBUS()
        raw_data = rebus.download(
            url="https://wlcg-cric.cern.ch/api/core/federation/query/?json"
        )
        json_data = rebus.convert(data=raw_data, sort_field="rcsites")
        if rebus.save(file="data/scraped_rebus_federations.json", data=json_data):
            logger.info("Scraped federations CRIC")
        else:
            logger.error("Problem scraping federations CRIC")

        # then the pledges
        # can actually use same JSON raw data as before
        json_data = rebus.convert(
            data=raw_data, sort_field="accounting_name", append_mode=True
        )
        if rebus.save(file="data/scraped_rebus_pledges.json", data=json_data):
            logger.info("Scraped pledges CRIC")
        else:
            logger.error("Problem scraping pledges CRIC")

        # we also get datadisk information from monit Grafana
        url = config.get("credentials_monit_grafana", "url")
        token = config.get("credentials_monit_grafana", "token")

        now = int(round(time.time() * 1000))
        date_to = now - 12 * 60 * 60 * 1000
        date_from = date_to - 24 * 60 * 60 * 1000

        period = """"gte":{0},"lte":{1}""".format(date_from, date_to)

        data = (
            """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_rucioacc_enr_site*"]}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"""
            + period
            + ""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"data.account:* AND data.campaign:* AND data.country:* AND data.cloud:* AND data.datatype:* AND data.datatype_grouped:* AND data.prod_step:* AND data.provenance:* AND data.rse:* AND data.scope:* AND data.experiment_site:* AND data.stream_name:* AND data.tier:* AND data.token:(\\\"ATLASDATADISK\\\" OR \\\"ATLASSCRATCHDISK\\\") AND data.tombstone:(\\\"primary\\\" OR \\\"secondary\\\") AND NOT(data.tombstone:UNKNOWN) AND data.rse:/.*().*/ AND NOT data.rse:/.*(none).*/"}}]}},"aggs":{"4":{"terms":{"field":"data.rse","size":500,"order":{"_term":"desc"},"min_doc_count":1},"aggs":{"1":{"sum":{"field":"data.files"}},"3":{"sum":{"field":"data.bytes"}}}}}}\n"""
        )

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer %s" % token,
        }

        grafana = Grafana(url=url, request=data, headers=headers)
        raw_data = grafana.download()
        pprint.pprint(raw_data)
        json_data = grafana.convert(data=raw_data.json())
        if grafana.save(file="data/scraped_grafana_datadisk.json", data=json_data):
            logger.info("Scraped datadisks from monit grafana")
        else:
            logger.error("Problem scraping datadisks from monit grafana")

        # TODO: not running ES scraper for now since the benchmark jobs are no longer being run
        # #get credentials
        # password = config.get("credentials_elasticsearch", "password")
        # username = config.get("credentials_elasticsearch", "username")
        # host = config.get("credentials_elasticsearch", "host")
        # arg = ([{'host': host, 'port': 9200}])
        # elasticsearch = ElasticSearch(arg,**{'http_auth':(username, password)})
        # kwargs = {
        #     'index' : "benchmarks-*",
        #     'body' : {
        #         "size" : 10000,"query" : {"match_all" : {},},
        #         "collapse": {"field": "metadata.PanDAQueue","inner_hits": {"name": "most_recent","size": 50,"sort": [{"timestamp": "desc"}]}
        #         }
        #     },
        #     'filter_path' : [""]
        # }
        # raw_data = elasticsearch.download(**kwargs)
        # json_data = elasticsearch.convert(data=raw_data)
        #
        # if elasticsearch.save(file='data/scraped_elasticsearch_benchmark.json', data=json_data):
        #     logger.info('Scraped benchmark results from ES')
        # else:
        #     logger.error('Problem scraping benchmark results from ES')

    else:
        # Nothing to do otherwise
        print("Dropping out")


if __name__ == "__main__":
    try:
        run()
    except Exception, e:
        logger.error("Got error while running scrapers. " + str(e))
        msg = "QMonit failed to run a scraper job.\n\nError:\n" + str(e)
        subj = "[QMonit error] InfluxDB"
        notifications.send_email(
            message=msg,
            subject=subj,
            **{"password": config.get("credentials_adcmon", "password")}
        )
