import unittest
import itertools
import os

from .helpers.JSONTest import JSONTest

from scrapers.agis import AGIS

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


if __name__ == "__main__":

    unittest.main()
