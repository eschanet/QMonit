import os
import json
from pprint import pprint
import collections
import datetime
from . import ElasticSearchScraper

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class ElasticSearch(ElasticSearchScraper):

    def __init__(self, host=[],**kwargs):
        super(ElasticSearch, self).__init__(host,**kwargs)

    def convert(self, data, *args, **kwargs):
        """Convert the Grafana data to the desired format of being ordered by datadisk names

        :param data: data to be converted in the desired format"""

        # all of this is still quite ugly and verrrry specific...
        json_data = {}
        for hit in data['hits']['hits']:
            # pprint(hit)

            # get the PQ
            pq = hit.get('_source',{}).get('metadata',{}).get('PanDAQueue',None)
            if not pq:
                continue

            # get the list of all benchmark results
            latest_list = hit.get('inner_hits',{}).get('most_recent',{}).get('hits',{}).get('hits',[])
            if len(latest_list)==0:
                continue

            # get the average of the latest benchmark results.
            # Only results not older than 7d, and a maximum of 50 results (whichever value is hit first).
            # If we have no values more recent than 7d, simply use the last available one (that PQ is probably not online anymore anyway)
            values=[]
            for d in latest_list:
                date = datetime.datetime.strptime( d.get('_source',{}).get('timestamp',""), '%Y-%m-%dT%H:%M:%SZ')
                two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
                seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)

                if date > two_days_ago:
                    # we are within the last two days, so we take all the measurements we can get!
                    values.append(d)
                elif (date < two_days_ago) and (date > seven_days_ago):
                    # we are between 2 and 7 days ago, so take only values if we don't have 25 values already
                    if len(values) < 30 :
                        values.append(d)
                elif date < seven_days_ago:
                    # we are further away than 7 days, so take a maximum of 5 values from here if we don't have 5 yet
                    if len(values) < 10:
                        values.append(d)

            to_average = [i.get('_source',{}).get('profiles',{}).get('fastBmk',{}).get('value',0.0) for i in values]
            json_data[pq] = {"avg_value" : float(sum(to_average))/len(to_average), "measurements" : len(to_average) }
            # print(len(to_average))

        return json_data
