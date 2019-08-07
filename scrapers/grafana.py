import os
import json
from pprint import pprint
import collections

from . import HTTPScraper

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class Grafana(HTTPScraper):

    def __init__(self, *args, **kwargs):
         super(Grafana, self).__init__(*args, **kwargs)

    def convert(self, data, append_mode=False,sort_field="panda_queue", should_be_ordered_by="panda_queue", *args, **kwargs):
        """Convert the Grafana data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        json_data = RebusDict()

        if isinstance(data,dict):
            for key,d in data.items():
                if isinstance(d.get(sort_field,{}), collections.Hashable):
                    json_data.update(object={d[sort_field]:d},append_mode=append_mode)
        elif isinstance(data,list):
            for d in data:
                if isinstance(d.get(sort_field,{}), collections.Hashable):
                    json_data.update(object={d[sort_field]:d},append_mode=append_mode)
        else:
            logger.error("Data is not type dict or list but: {}".format(type(data)))

        return json_data
