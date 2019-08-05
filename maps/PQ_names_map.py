import os
import json
from maps import Map

class PQ_names_map(Map):

    def __init__(self, file, *args, **kwargs):
        super(PQ_names_map, self).__init__(*args, **kwargs)

        data = self.load_file(file)

    def update(self,ifile,ofile,key):
        '''Update the PQ names map

        :param ifile: the name of the inputfile
        :param ofile: the name of the outpufile for the map
        :param key: keyword in the ifile that should be used to create the actual map'''
        with open(ifile) as f:
            map = {}
            data = json.load(f)
            for kw,val in data.iteritems():
                map[val["panda_resource"]] = kw

            return self.save(ofile, map)
