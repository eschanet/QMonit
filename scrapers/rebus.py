import os
import json

from . import Scraper
from maps import PQ_names_map as pq_map

class REBUS(Scraper):

    def __init__(self, *args, **kwargs):
         super(REBUS, self).__init__(*args, **kwargs)

    def convert(self, data, sort_field="panda_queue", should_be_ordered_by="panda_queue", *args, **kwargs):
        """Convert the REBUS data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        # We need a map between PQ names and the names that are used elsewhere. Weirdly enough, there are different variationsself.
        pqs = pq_map.PQ_names_map()

        json_data={}

        for key,d in data.items():
            json_data[d[sort_field]] = d

        return json_data
