import os
import json
from pprint import pprint
import collections

from . import Scraper

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class RebusDict(dict):
    def __init__(self, *args, **kwargs):
        super(RebusDict, self).__init__(*args, **kwargs)

    def update(self,object,append_mode=False):
        if not append_mode:
            return super(RebusDict, self).update(object)
        elif append_mode:
            k,v = object.items()[0]
            if k in self:
                self[k].append(v)
            else:
                self[k] = [v]
        else:
            super(RebusDict, self).update(object)

class REBUS(Scraper):

    def __init__(self, *args, **kwargs):
         super(REBUS, self).__init__(*args, **kwargs)

    def convert(self, data, append_mode=False,sort_field="panda_queue", should_be_ordered_by="panda_queue", *args, **kwargs):
        """Convert the REBUS data to the desired format of being ordered by Panda queues

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
