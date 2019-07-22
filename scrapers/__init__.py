import os

import abc

class Scraper(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        """Initializing the scraper object."""

    @classmethod
    def create_dir(path):
        """Create directory with path.

        :param path: directory path"""

        try:
            os.makedirs(path)
        except OSError as error:
            if error.errno != 17:
                raise

    @classmethod
    def save_json_to_file(filename,data):
        try:
            with open(filename, 'w') as f:
                json.dump(data, f)
            return True
        except IOError:
            print("Got an error saving to file.")
            return False


    @abc.abstractmethod
    def write(self, data, path, *args, **kwargs):
        """Writes data to path.

        :param data: input data
        :param path: output path
        :param args: additional arguments that are passed to the class
        :param kwargs: additional arguments that are passed to the class"""
