from __future__ import print_function

import os
import urllib
import json

import abc

class Scraper(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, url, output_path, output_file, *args, **kwargs):
        """Initializing the scraper object."""
        self.url = url
        self.output_file = output_file
        self.output_path = output_path

    @classmethod
    def create_dir(path):
        """Create directory with path.

        :param path: directory path"""

        try:
            os.makedirs(path)
        except OSError as error:
            if error.errno != 17:
                raise

    def save_data(file,data):
        try:
            with open(file, 'w') as f:
                json.dump(data, f)
            return True
        except IOError:
            print("Got an error saving to file.")
            return False

    def download(url):
        """Download JSON data from url.

        :param url: the url containing the JSON to be downloaded."""

        response = urllib.urlopen(url)
        data = json.load(response)
        return data

    @abc.abstractmethod
    def convert(self, data, *args, **kwargs):
        """Converts downloaded data to desired output format.

        :param data: input data that has been downloaded
        :param args: additional arguments that are passed to the class
        :param kwargs: additional arguments that are passed to the class"""
