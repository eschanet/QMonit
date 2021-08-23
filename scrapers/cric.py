import os
import json

from . import JSONScraper

from commonHelpers.logger import logger

logger = logger.getChild(__name__)


class CRIC(JSONScraper):
    def __init__(self, *args, **kwargs):
        super(CRIC, self).__init__(*args, **kwargs)

    def convert(
        self,
        data,
        sort_field="panda_queue",
        should_be_sorted_by="panda_queue",
        *args,
        **kwargs
    ):
        """Convert the CRIC data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        json_data = {}

        if isinstance(data, dict):
            for key, d in data.items():
                if sort_field in d:
                    json_data[d[sort_field]] = d
        elif isinstance(data, list):
            for d in data:
                if sort_field in d:
                    json_data[d[sort_field]] = d
        else:
            logger.error("Data is not type dict or list but: {}".format(type(data)))

        return json_data
