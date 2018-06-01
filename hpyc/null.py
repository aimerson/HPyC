#! /usr/bin/env python

import sys,os,fnmatch,re
import numpy as np
import getpass,subprocess,glob
from .jobs import getBatchVariable,JOBCLASS

class NULLjob(JOBCLASS):    
    def __init__(self,verbose=False):
        super(NULLjob, self).__init__(None,verbose=verbose)
        self.interactive = True
        self.jobName = None
        self.jobID = None
        self.jobArray = False
        self.jobArrayID = None
        self.jobArrayIndex = None
        self.taskID = None
        self.minJobArrayID = None
        self.maxJobArrayID = None
        self.minTaskID = None
        self.maxTaskID = None
        self.account = None
        self.userID = os.environ["USER"]
        self.machine = os.environ['HOST']
        self.queue = None
        self.jobStatus = None
        self.walltime = None
        self.nodefile = None
        self.nodes = 1
        if "OMP_NUM_THREADS" in os.environ.keys():
            self.cpus = int(os.environ["OMP_NUM_THREADS"])
        else:
            self.cpus = 1
        self.ppn = self.cpus
        self.submitDir = subprocess.check_output(["pwd"]).replace("\n","")
        self.workDir = subprocess.check_output(["pwd"]).replace("\n","")
        return

