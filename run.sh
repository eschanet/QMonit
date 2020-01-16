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

#run the scrapers
python run_scrapers.py -interval 10m

#use the updated queue info and write it into MySQL database
python writeQueueInfoMYSQL.py

#get data from panda and write to influxdb
python writeJobStats.py

#keep load of influx and compute interesting quantities offline
#download data, compute quantities, re-upload with same timestamp
python writeDerivedQuantities.py
