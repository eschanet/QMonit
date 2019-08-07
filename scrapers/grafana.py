import os
import json
from pprint import pprint
import collections

from . import HTTPScraper

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class Grafana(HTTPScraper):

    def __init__(self, headers, request, url):
        super(Grafana, self).__init__(request=request, url=url, headers=headers)

    def convert(self, data, *args, **kwargs):
        """Convert the Grafana data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        # all of this is still quite ugly ...
        json_data = {}

        for k in json.loads(data.text)['responses'][0]['aggregations']['4']['buckets']:
            rse = k['key']
            files = int(k['1']['value'])
            bytes = int(k['3']['value'])

            json_data[rse] = {'bytes': bytes, 'files': files}

        return json_data
