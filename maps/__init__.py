import os

import abc

class Map(dict):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*arg, **kw)
