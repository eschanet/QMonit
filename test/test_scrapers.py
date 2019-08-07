import unittest
import itertools
import os
from pprint import pprint

from .helpers.JSONTest import JSONTest

from scrapers.agis import AGIS
from scrapers.rebus import REBUS

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
        my_data = rebus.convert(data=original_data,sort_field="Site")
        self.assertSame(output_data, my_data)

    def test_pledge_scraper_conversion(self):
        """
        Test that converting scraped REBUS pledges works
        """
        original_data = fh.get_json_from_file('test/references/test_input_rebus_pledges.json')
        output_data = fh.get_json_from_file('test/references/test_output_rebus_pledges.json')
        rebus = REBUS()
        my_data = rebus.convert(data=original_data,sort_field="Federation", append_mode=True)
        self.assertSame(output_data, my_data)


if __name__ == "__main__":

    unittest.main()
