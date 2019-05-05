#!/bin/bash

#need python 2.7
export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
lsetup python

#execute script
cd /afs/cern.ch/user/e/eschanet/qualitask/

#new manual downsampling
python downsample.py -average 1d
