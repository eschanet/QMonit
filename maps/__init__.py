import os
import json

import abc

class Map(dict):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)

    def load_file(self,file):
        try:
            with open(file) as json_file:
                data = json.load(json_file)
                return data
        except:
            raise
