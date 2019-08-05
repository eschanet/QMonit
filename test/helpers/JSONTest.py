import unittest
import itertools
import os
import shutil

from commonHelpers.logger import logger
logger = logger.getChild(__name__)

class JSONTest(unittest.TestCase):

    # this should work, thanks: https://stackoverflow.com/a/25851972
    @classmethod
    def ordered(self,obj):
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj

    def assertSame(self, a, b):
        """
        Asserts if a and b are the same
        """
        return self.assertEqual(self.ordered(a),self.ordered(b))
