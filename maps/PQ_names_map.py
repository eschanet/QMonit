import os

from maps import Map

class PQ_names_map(Map):

    def __init__(self, *args, **kwargs):
        super(PQ_names_map, self).__init__(*args, **kwargs)

        data = self.load_file(*args, **kwargs)
        self.update(data)

    def load_file(self, *args,**kwargs):
        filename = kwargs.get('filename',"PQ_names_map.json")
        return super(PQ_names_map,self).load_file(filename)
