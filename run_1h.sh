#!/bin/bash

#need python 2.7
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
lsetup python

#execute script
cd /afs/cern.ch/user/e/eschanet/qualitask/

#get updated DAOD datadisk info
python downloadDatadiskInfo.py

#get DDM Information
python downloadDDMInfo.py

#get ATLAS site info
python downloadSiteInfo.py

#write datadisk time-series info
python writeDatadiskInfo.py
