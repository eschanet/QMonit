from requests import post
from json import loads, dumps, dump
import json
import time

def run_query():
    base = "https://monit-grafana.cern.ch"
    url = "api/datasources/proxy/9037/_msearch"
    db = "monit_production_ddm_transfers"

    token = "***REMOVED***"

    now = int(round(time.time() * 1000))
    yesterday = now - 12*60*60*1000
    two_days = yesterday - 24*60*60*1000

    # print now
    # print yesterday

    data = '{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_rucioacc_enr_site_*","monit_prod_rucioacc_enr_site_*"]}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":"%i","lte":"%i","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"data.account:* AND data.campaign:* AND data.country:* AND data.cloud:* AND data.datatype:* AND data.datatype_grouped:(\\"DAOD\\") AND data.prod_step:* AND data.provenance:* AND data.rse:* AND data.scope:* AND data.experiment_site:* AND data.stream_name:* AND data.tier:* AND data.token:(\\"ATLASDATADISK\\" OR \\"T2ATLASDATADISK\\") AND data.tombstone:* AND NOT(data.tombstone:UNKNOWN) AND data.rse:/.*().*/ AND NOT data.rse:/.*(none).*/"}}]}},"aggs":{"4":{"terms":{"field":"data.rse","size":500,"order":{"_term":"desc"},"min_doc_count":1},"aggs":{"1":{"sum":{"field":"data.files"}},"3":{"sum":{"field":"data.bytes"}}}}}}\n' % (two_days,yesterday)
    headers = {'Authorization': 'Bearer %s' % token}

    request_url = "%s/%s" % (base, url)

    r = post(request_url, headers=headers, data=data)

    data = {}
    for k in loads(r.text)['responses'][0]['aggregations']['4']['buckets']:
        rse = k['key']
        files = int(k['1']['value'])
        bytes = int(k['3']['value'])

        data[rse] = {'bytes': bytes, 'files': files}
        print k


    try:
        with open("daods_datadisk.json", 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)
        return True
    except IOError:
        print("Got an error saving to file.")
        return False

    # print dumps(data, indent=4, sort_keys=True)

if __name__ == "__main__":
    run_query()
