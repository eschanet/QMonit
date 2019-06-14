import urllib
import json

def get_json_from_url(url):
    response = urllib.urlopen(url)
    data = json.load(response)
    return data

def save_json_to_file(filename,data):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
        return True
    except IOError:
        print("Got an error saving to file.")
        return False
