#! /usr/bin/env python
"""
qsub_example.py -- submit an example PBS job

USAGE: ./qsub_example.py [-s <scratchPath>] [-q <queue>] [-J <arrayOptions>] 
                         [-n <jobname>] [-l <resource>] [-logs] [-SUBMIT]

"""
import sys,os,getpass,fnmatch,subprocess,glob
import numpy as np
import datetime
from hpyc.pbs import submitPBS

# If no arguments specified, print docstring and quit
if len(sys.argv) == 1:
    print(__doc__)
    quit()

USER = getpass.getuser()
pwd = subprocess.check_output(["pwd"]).replace("\n","")

# Initialise PBS class
SUBMIT = submitPBS(overwrite=True)

# Get arguments
JOBNAME = "myExampleJob"
rmlogs = False
SCRATCH = None
RUNS = None
QUEUE = None
SUBMIT_JOB_TO_QUEUE = False
iarg = 0
while iarg < len(sys.argv):
    if fnmatch.fnmatch(sys.argv[iarg],"-s*"):
        iarg += 1
        SCRATCH = sys.argv[iarg]
    if fnmatch.fnmatch(sys.argv[iarg],"-n*"):
        iarg += 1
        JOBNAME = sys.argv[iarg]
    if fnmatch.fnmatch(sys.argv[iarg],"-logs"):
        rmlogs = True
    if fnmatch.fnmatch(sys.argv[iarg],"-SUBMIT"):
        SUBMIT_JOB_TO_QUEUE = True
    if fnmatch.fnmatch(sys.argv[iarg],"-q"):
        iarg += 1
        QUEUE = sys.argv[iarg]
    if fnmatch.fnmatch(sys.argv[iarg],"-J"):
        iarg += 1
        RUNS = sys.argv[iarg]
    if fnmatch.fnmatch(sys.argv[iarg],"-l"):
        iarg += 1
        SUBMIT.addResource(sys.argv[iarg])        
    iarg += 1

# Set path to output and log directories
if SCRATCH is None:
    SCRATCH = "."
if not SCRATCH.endswith("/"):
    SCRATCH = SCRATCH+"/"
# Create logs directory
LOGDIR = SCRATCH+"Logs/pbsExample/"
subprocess.call(["mkdir","-p",LOGDIR])        
if rmlogs:
    for logfile in glob.glob(LOGDIR+"*"):
        os.remove(logfile)
SUBMIT.addOutputPath(LOGDIR)
SUBMIT.addErrorPath(LOGDIR)
SUBMIT.joinOutErr()


ARGS = {"EXAMPLE_ARGUMENT_1":"helloWorld"}
ARGS["EXAMPLE_ARGUMENT_2"] = 3.14159

# Set remaing PBS options
SUBMIT.addQueue(QUEUE)
SUBMIT.addJobName(JOBNAME)
SUBMIT.specifyJobArray(RUNS)
ARGS["JOB_ARRAY_SIZE"] = SUBMIT.countJobs()
ARGS["JOB_MANAGER"] = SUBMIT.manager
SUBMIT.passScriptArguments(ARGS)
SUBMIT.setScript(os.path.basename(__file__).replace("qsub","run"))

# Submit job
SUBMIT.printJobString()
if SUBMIT_JOB_TO_QUEUE:
    print("Submitting example job...")
    SUBMIT.submitJob()








