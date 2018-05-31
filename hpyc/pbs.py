#! /usr/bin/env python

import sys,os,fnmatch,re
import numpy as np
import getpass,subprocess,glob
from .batchJobs import getBatchVariable,JOBCLASS,NULLjob

########################################################################################################
#   PBS classes/functions
########################################################################################################


class PBSjob(JOBCLASS):
    
    def __init__(self,verbose=False):
        super(PBSjob, self).__init__("PBS",verbose=verbose)
        # Get job name and ID and identify whether job array
        self.jobName = self.getEnvironmentVariable("PBS_JOBNAME")
        if self.jobName is None or self.jobName == "STDIN":
            self.interactive = True
        else:
            self.interactive = False
        # Get job ID
        self.jobID = self.getEnvironmentVariable("PBS_JOBID")
        # Get job array information
        self.jobArrayID = self.getEnvironmentVariable("PBS_ARRAY_ID")
        self.jobArrayIndex = self.getEnvironmentVariable("PBS_ARRAY_INDEX")        
        if self.jobArrayIndex:
            self.jobArray = True            
        else:
            self.jobArray = False
        self.minJobArrayID = None
        self.minTaskID = None
        self.maxJobArrayID = None
        self.maxTaskID = None
        # Get user ID
        self.userID = self.getEnvironmentVariable("PBS_O_LOGNAME")
        # Get machine and queue
        self.machine = self.getEnvironmentVariable("PBS_O_HOST")
        self.queue = self.getEnvironmentVariable("PBS_QUEUE")
        # Get submission dir
        self.submitDir = self.getEnvironmentVariable("PBS_O_WORKDIR")
        self.workDir = self.getEnvironmentVariable("PBS_O_WORKDIR")
        # Query qstat and store result
        self.jobStatus = subprocess.check_output(["qstat","-f",self.jobID]).split("\n")
        # Get nodes resource information
        self.nodefile = self.getEnvironmentVariable("PBS_NODEFILE")
        self.nodes = int(fnmatch.filter(self.jobStatus,"*Resource_List.nodect =*")[0].split("=")[-1].strip())
        self.cpus = int(fnmatch.filter(self.jobStatus,"*Resource_List.ncpus =*")[0].split("=")[-1].strip())
        self.ppn = int(float(self.cpus)/float(self.nodes))
        # Get walltime
        self.walltime = fnmatch.filter(self.jobStatus,"*Resource_List.walltime =*")[0].split("=")[-1].strip()
        return


class submitPBS(JOBCLASS):
    
    def __init__(self,verbose=False,overwrite=False):
        super(submitPBS, self).__init__("PBS",verbose=verbose)
        self.cmd = "qsub -V"
        self.appendable = True
        self.overwrite = overwrite
        return

    def canAppend(self):
        if not self.appendable:
            print("PBS submission string is not appendable!")
        return self.appendable

    def replaceOption(self,old,new):
        S = re.search(old,self.cmd)
        if S:
            self.cmd = self.cmd.replace(old,new)
        return

    def addShell(self,shell):
        if shell is None: return
        S = re.search(' -S (\w+) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Shell already specified! Shell = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -S "+shell)
        else:            
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -S "+shell+" "
        return
            
    def addQueue(self,queue):
        if queue is None: return
        S = re.search(' -q (\w+) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Queue already specified! Queue = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -q "+queue)
        else:            
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -q "+queue+" "
        return

    def addJobName(self,name):
        if name is None: return
        S = re.search(' -N (\w+) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Job name already specified! Job Name = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -N "+name)                
        else:
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -N "+name+" "
        return

    def addAccount(self,account):
        if account is None: return
        S = re.search(' -A (\w+) ',self.cmd)
        if S: 
            if not self.overwrite:
                print("Account already specified! Account = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -A "+account)                
        else:
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -A "+account+" "
        return

    def addResource(self,resourceStr):
        if not self.canAppend(): return            
        self.cmd = self.cmd + " -l "+resourceStr
        return

    def addOutputPath(self,outPath):
        if outPath is None: return
        S = re.search(' -o (\S*) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Output path already specified! Output path = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -o "+outPath)                
        else:
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -o "+outPath+" "
        return

    def addErrorPath(self,errPath):
        if errPath is None: return
        S = re.search(' -e (\S*) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Error path already specified! Error path = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -e "+errPath)                
        else:
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -e "+errPath+" "
        return

    def joinOutErr(self):
        if not self.canAppend(): return            
        S = re.search(' -j oe ',self.cmd)
        if not S:
            self.cmd = self.cmd + " -j oe "
        return

    def specifyJobArray(self,arrayString):
        if arrayString is None: return
        S = re.search(' -J (\S*) ',self.cmd)
        if S:
            if not self.overwrite:
                print("Job array options already specified! Job array options= "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," -J "+arrayString)                
        else:
            if not self.canAppend(): return            
            self.cmd = self.cmd + " -J "+arrayString+" "
        return

    def countJobs(self):
        nJobs = 1
        S = re.search(' -J (\S*) ',self.cmd)
        if S:
            T = re.match('(\d+)-(\d+)?(:\d+)?',S.group(1))
            start = int(T.group(1))
            end = int(T.group(2))+1
            if T.group(3):
                step = int(T.group(3).replace(":",""))               
            else:
                step = 1
            nJobs = len(np.arange(start,end,step))
        return nJobs

    def passScriptArguments(self,args):
        if args is None: return
        S = re.search(' -v (\S*) ',self.cmd)        
        if S:
            existing = {}
            for obj in S.group(1).split(","):
                existing[obj.split("=")[0]] = obj.split("=")[1]
            keys = list(map(str,list(set(args.keys() + existing.keys()))))
            argString = None
            for key in keys:
                if key in args.keys() and key in existing.keys():
                    if self.overwrite:
                        thisArg = key+"="+str(args[key])
                    else:
                        thisArg = key+"="+str(existing[key])
                else:                    
                    if key in args.keys():
                        thisArg = key+"="+str(args[key])
                    if key in existing.keys():
                        thisArg = key+"="+str(existing[key])
                if argString is None:
                    argString = thisArg
                else:
                    argString = argString+","+thisArg
            self.replaceOption(S.group(0)," -v "+argString)
        else:
            if not self.canAppend(): return            
            argString = ",".join([str(key)+"="+str(args[key]) for key in args.keys()])
            self.cmd = self.cmd + " -v "+argString+" "
        return
            
    def setScript(self,script):
        if script is None: return
        if not self.canAppend(): return            
        appendable = False
        self.cmd = self.cmd + " " +script+" "
        return

    def printJobString(self):
        print(self.cmd.replace("  "," "))
        return 

    def getJobString(self):
        return self.cmd.replace("  "," ")

    def submitJob(self):
        os.system(self.cmd.replace("  "," "))
        return

