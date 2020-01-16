#titan_testScript_ec2_alice_1.py                                                
#                                                                               
import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec

aSrvID = None


for idx,argv in enumerate(sys.argv):
    if argv == '-s':
        aSrvID = sys.argv[idx+1]
        sys.argv = sys.argv[:idx]
        break

site = sys.argv[1]

datasetName = 'panda.destDB.%s' % str(uuid.uuid4())
destName    = 'local'

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s" % str(uuid.uuid4())
# MPI transform on Titan that will run actual job                               
job.transformation    = '/lustre/atlas/proj-shared/csc108/panitkin/alicetest1/m\
pi_wrapper_alice_ppbench.py'

job.destinationDBlock = datasetName
job.destinationSE     = destName
job.currentPriority   = 1000
job.prodSourceLabel   = 'panda'
job.computingSite     = site
job.jobParameters     = " "
job.VO                = 'alice'

fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE     = job.destinationSE
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)


s,o = Client.submitJobs([job],srvID=aSrvID)
print(s)
for x in o:
    print("PandaID=%s" % x[0])
