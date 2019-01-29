#!/usr/bin/python3.4

from __future__ import print_function

from pprint import pprint
import json,sys

with open('pandaqueue.json') as json_data:
    panda_queues = json.load(json_data)

panda_resources = {}
for queue,values in panda_queues.iteritems():
    panda_resources[values["panda_resource"]] = queue
    print(queue)

with open('pandaresource.json', 'w') as out:
    json.dump(panda_resources, out)
