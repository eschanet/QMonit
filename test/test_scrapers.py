import unittest
import itertools
import os
from pprint import pprint

from .helpers.JSONTest import JSONTest

from scrapers.agis import AGIS
from scrapers.rebus import REBUS
from scrapers.grafana import Grafana
from scrapers.elasticsearch import ElasticSearch

from commonHelpers import fileHelpers as fh
from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class TestAGIS(JSONTest):

    def test_site_scraper_conversion(self):
        """
        Test that converting scraped AGIS sites works
        """
        original_data = fh.get_json_from_file('test/references/test_input_agis_site.json')
        output_data = fh.get_json_from_file('test/references/test_output_agis_site.json')
        agis = AGIS()
        my_data = agis.convert(data=original_data,sort_field="name")
        self.assertSame(output_data, my_data)

    def test_queue_scraper_conversion(self):
        """
        Test that converting scraped AGIS panda queues works
        """
        original_data = fh.get_json_from_file('test/references/test_input_agis_pandaqueue.json')
        output_data = fh.get_json_from_file('test/references/test_output_agis_pandaqueue.json')
        agis = AGIS()
        my_data = agis.convert(data=original_data,sort_field="panda_resource")
        self.assertSame(output_data, my_data)

    def test_ddm_scraper_conversion(self):
        """
        Test that converting scraped AGIS panda queues works
        """
        original_data = fh.get_json_from_file('test/references/test_input_agis_ddm.json')
        output_data = fh.get_json_from_file('test/references/test_output_agis_ddm.json')
        agis = AGIS()
        my_data = agis.convert(data=original_data,sort_field="site")
        self.assertSame(output_data, my_data)

class TestREBUS(JSONTest):

    def test_federation_scraper_conversion(self):
        """
        Test that converting scraped REBUS federations works
        """
        original_data = fh.get_json_from_file('test/references/test_input_rebus_federation.json')
        output_data = fh.get_json_from_file('test/references/test_output_rebus_federation.json')
        rebus = REBUS()
        my_data = rebus.convert(data=original_data,sort_field="rcsites")
        self.assertSame(output_data, my_data)

    def test_pledge_scraper_conversion(self):
        """
        Test that converting scraped REBUS pledges works
        """
        original_data = fh.get_json_from_file('test/references/test_input_rebus_pledges.json')
        output_data = fh.get_json_from_file('test/references/test_output_rebus_pledges.json')
        rebus = REBUS()
        my_data = rebus.convert(data=original_data,sort_field="accounting_name", append_mode=True)
        self.assertSame(output_data, my_data)

class TestGrafana(JSONTest):

    def test_datadisk_scraper_conversion(self):
        """
        Test that converting scraped datadisk information from Grafana works
        """
        original_data = fh.get_json_from_file('test/references/test_input_grafana_datadisks.json')
        output_data = fh.get_json_from_file('test/references/test_output_grafana_datadisks.json')

        grafana = Grafana(url="",request="",headers={})

        my_data = grafana.convert(data=original_data)
        self.assertSame(output_data, my_data)

class TestElasticSearch(JSONTest):

    def test_benchmark_scraper_conversion(self):
        """
        Test that converting the scraped benchmark job tests from ES works fine.
        """
        original_data = fh.get_json_from_file('test/references/test_input_elasticsearch_benchmarks.json')
        output_data = fh.get_json_from_file('test/references/test_output_elasticsearch_benchmarks.json')

        elasticsearch = ElasticSearch()

        my_data = elasticsearch.convert(data=original_data)
        self.assertSame(output_data, my_data)

if __name__ == "__main__":

    unittest.main()
