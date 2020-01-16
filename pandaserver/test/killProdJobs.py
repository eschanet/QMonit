import sys

import pandaserver.userinterface.Client as Client

if len(sys.argv) == 2:
    jobDefIDs = [sys.argv[1]]
else:
    startID = int(sys.argv[1])
    endID   = int(sys.argv[2])
    if startID > endID:
        print('%d is less than %d' % (endID,startID))
        sys.exit(1)
    jobDefIDs = range(startID,endID+1)
    
# quesry PandaID
status, ids = Client.queryPandaIDs(jobDefIDs)

if status != 0:
    sys.exit(0)
    
# remove None
while True:
    if not None in ids:
        break
    ids.remove(None)

# kill
if len(ids) != 0:
    Client.killJobs(ids)
