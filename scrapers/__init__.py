from __future__ import print_function

import os
import urllib
import json
from requests import post

import abc

class Scraper(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        """Initializing the scraper object."""

    @staticmethod
    def create_dir(path):
        """Create directory with path.

        :param path: directory path"""

        try:
            os.makedirs(path)
        except OSError as error:
            if error.errno != 17:
                raise

    @staticmethod
    def save(file,data):
        try:
            with open(file, 'w') as f:
                json.dump(data, f)
            return True
        except IOError:
            print("Got an error saving to file.")
            return False

    @abc.abstractmethod
    def download(self,request,token):
        """
        Download JSON data from url.
        """

    @abc.abstractmethod
    def convert(self, data, *args, **kwargs):
        """Converts downloaded data to desired output format.

        :param data: input data that has been downloaded
        :param args: additional arguments that are passed to the class
        :param kwargs: additional arguments that are passed to the class"""


class HTTPScraper(Scraper):
    __metaclass__ = abc.ABCMeta

    def __init__(self, token, request, url, *args, **kwargs):
        """Initializing the scraper object."""
        super(HTTPScraper, self).__init__(*args, **kwargs)

        self.token = token
        self.request = request
        self.url = url


    def download(self):
        """
        Download data.
        """
        return post(self.url, headers=self.headers, data=self.request)


class JSONScraper(Scraper):
    __metaclass__ = abc.ABCMeta

    def __init__(self, *args, **kwargs):
        """Initializing the scraper object."""
        super(JSONScraper, self).__init__(*args, **kwargs)

    def download(self,url):
        """Download JSON data from url.

        :param url: the url containing the JSON to be downloaded."""

        response = urllib.urlopen(url)
        data = json.load(response)
        return data
