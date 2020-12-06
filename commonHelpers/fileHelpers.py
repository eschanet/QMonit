import urllib.request
import json

def get_json_from_file(file):
    with open(file) as f:
        return json.load(f)

def get_json_from_url(url):
    response = urllib.request.urlopen(url)
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
