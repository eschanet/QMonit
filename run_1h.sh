#!/bin/bash

#need python 2.7
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
lsetup python

#execute script
if [ "$(whoami)" = "adcmon" ]; then
  dir="/afs/cern.ch/user/a/adcmon/private/QMonit/"
else
  dir="/afs/cern.ch/user/e/eschanet/queue_monit/"
fi
cd ${dir}

#run scrapers
python run_scrapers.py -interval 1h

#write datadisk time-series info
python writeDatadiskInfo.py
