#!/bin/bash

#need python 2.7
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
lsetup python

#execute script
cd /afs/cern.ch/user/e/eschanet/queue_monit/

#get updated DAOD datadisk info
python downloadDatadiskInfo.py

#get DDM Information
python downloadDDMInfo.py

#get ATLAS site info
python downloadSiteInfo.py

#write datadisk time-series info
python writeDatadiskInfo.py

#get some information from wlcg rebus
#first the actual federations map
python downloadFederationMap.py
#next the federation pledges
python downloadFederationPledges.py

#download ES benchmark data
python downloadElasticSearch.py
