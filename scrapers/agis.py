import os
import json

from . import Scraper
from maps import PQ_names_map as pq_map

class AGIS(Scraper):

    def __init__(self, *args, **kwargs):
         super(AGIS, self).__init__(*args, **kwargs)

    def convert(self, data, sort_field="panda_queue", should_be_sorted_by="panda_queue", *args, **kwargs):
        """Convert the AGIS data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        json_data={}

        for key,d in data.items():
            json_data[d[sort_field]] = d

        return json_data
