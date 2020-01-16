import sys
import traceback

from pandaserver.srvcore.CoreUtils import commands_get_status_output

# exec
def run(inFile,v_onlyTA,v_firstSubmission):
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    try:
        # read Jobs from file
        f = open(inFile, 'rb')
        jobs = pickle.load(f)
        f.close()
    except Exception as e:
        print("run() : %s %s" % (str(e), traceback.format_exc()))
        return
    # password
    from pandaserver.config import panda_config
    # initialize cx_Oracle using dummy connection
    from pandaserver.taskbuffer.Initializer import initializer
    initializer.init()
    # instantiate TB
    from pandaserver.taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
    # run Setupper
    from pandaserver.dataservice.Setupper import Setupper
    thr = Setupper(taskBuffer,jobs,onlyTA=v_onlyTA,firstSubmission=v_firstSubmission)
    thr.start()
    thr.join()
    return


# exit action
def _onExit(fname):
    commands_get_status_output('rm -rf %s' % fname)
        

####################################################################
# main
def main():
    import getopt
    import atexit
    # option class
    class _options:
        def __init__(self):
            pass
    options = _options()
    del _options
    # set default values
    options.inFile  = ""
    options.onlyTA  = False
    options.firstSubmission = True
    # get command-line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:tf")
    except Exception:
        print("ERROR : Invalid options")
        sys.exit(1)    
    # set options
    for o, a in opts:
        if o in ("-i",):
            options.inFile = a
        if o in ("-t",):
            options.onlyTA = True
        if o == "-f":
            options.firstSubmission = False
    # exit action
    atexit.register(_onExit,options.inFile)
    # run
    run(options.inFile,options.onlyTA,options.firstSubmission)
    # return
    sys.exit(0)


if __name__ == "__main__":
    main()
