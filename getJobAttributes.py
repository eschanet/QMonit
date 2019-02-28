#!/usr/bin/python

from __future__ import print_function

from pprint import pprint
import json,sys
import requests
import cPickle as pickle
from datetime import datetime

from pandaserver.taskbuffer import JobSpec
from pandaserver.userinterface import Client

job = Client.getJobStatus([4242299116])

spec = job[1][0]
att = spec.valuesMap()

pprint(att)
