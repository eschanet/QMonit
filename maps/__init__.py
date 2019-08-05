import os
import json

import abc

class Map(dict):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)

    def load_file(self,file):
        '''Load a file that contains the map'''
        try:
            with open(file) as json_file:
                data = json.load(json_file)
                return data
        except:
            raise

    def save(self,filename,data):
        try:
            with open(filename, 'w') as f:
                json.dump(data, f)
            return True
        except IOError:
            print("Got an error saving to file.")
            return False

    @classmethod
    def update(self,ifile,ofile,key):
        '''Update the map from some other input file

        :param ifile: name of the input file
        :param ofile: name of the output file '''
