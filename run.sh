#!/bin/bash

#need python 2.7
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
lsetup python

#execute script
cd /afs/cern.ch/user/e/eschanet/qualitask/

#get updated queue info from AGIS
python downloadQueueInfo.py

#use the updated queue info and write it into MySQL database
python writeQueueInfoMYSQL.py

#get data from panda and write to influxdb
python getJobStatsFlat.py

#keep load of influx and compute interesting quantities offline
#download data, compute quantities, re-upload with same timestamp
python writeDerivedQuantities.py
