import os
import json
from pprint import pprint
import collections

from . import JSONScraper

from commonHelpers.logger import logger

logger = logger.getChild("mephisto")


class RebusDict(dict):
    def __init__(self, *args, **kwargs):
        super(RebusDict, self).__init__(*args, **kwargs)

    def update(self, object, append_mode=False):
        def isATLAS(object):
            for key, item in object.items():
                if (
                    ("CMS Federation" in item.get("accounting_name", ""))
                    or ("LHCb Federation" in item.get("accounting_name", ""))
                    or ("ALICE Federation" in item.get("accounting_name", ""))
                ):
                    return False
                if (
                    (" CMS " in item.get("accounting_name", ""))
                    or (" LHCb " in item.get("accounting_name", ""))
                    or (" ALICE " in item.get("accounting_name", ""))
                ):
                    return False
                if "ATLAS" in item.get("accounting_name", ""):
                    return True
            return True

        # FIXME: this is ugly
        # Check if indeed ATLAS federation or maybe CMS/LHCb ...
        if not isATLAS(object):
            logger.debug("Not ATLAS")
            return self

        if not append_mode:
            return super(RebusDict, self).update(object)
        elif append_mode:
            k, v = object.items()[0]
            if k in self:
                self[k].append(v)
            else:
                self[k] = [v]
        else:
            super(RebusDict, self).update(object)


class REBUS(JSONScraper):
    def __init__(self, *args, **kwargs):
        super(REBUS, self).__init__(*args, **kwargs)

    def convert(
        self,
        data,
        append_mode=False,
        sort_field="panda_queue",
        should_be_ordered_by="panda_queue",
        *args,
        **kwargs
    ):
        """Convert the REBUS data to the desired format of being ordered by Panda queues

        :param data: data to be converted in the desired format"""

        json_data = RebusDict()

        if isinstance(data, dict):
            for key, d in data.items():
                if key == "NULL":
                    # CRIC has this huge NULL entry?!
                    continue
                if isinstance(d.get(sort_field, []), list):
                    for site in d.get(sort_field, []):
                        logger.debug("Adding {}".format(site))
                        logger.debug(d)
                        json_data.update(object={site: d}, append_mode=append_mode)
                elif isinstance(d.get(sort_field, {}), collections.Hashable):
                    logger.debug("Adding {}".format(d.get(sort_field, {})))
                    json_data.update(object={d[sort_field]: d}, append_mode=append_mode)
        else:
            logger.error("Data is not type dict or list but: {}".format(type(data)))

        return json_data
