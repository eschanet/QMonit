import os
import json
from pprint import pprint
import collections

from . import ElasticSearchScraper

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class ElasticSearch(ElasticSearchScraper):

    def __init__(self, *args,**kwargs):
        super(ElasticSearch, self).__init__(*args,**kwargs)

    def convert(self, data, *args, **kwargs):
        """Convert the Grafana data to the desired format of being ordered by datadisk names

        :param data: data to be converted in the desired format"""

        # all of this is still quite ugly and verrrry specific...
        json_data = {}
        responses = data.get('responses', [])
        if len(responses) > 0:
            for k in responses[0].get('aggregations',{}).get('4',{}).get('buckets',{}):
                rse = k['key']
                files = int(k['1']['value'])
                bytes = int(k['3']['value'])

                json_data[rse] = {'bytes': bytes, 'files': files}

        return json_data
