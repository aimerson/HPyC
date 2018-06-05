#! /usr/bin/env python

import sys,os,fnmatch,re
import numpy as np
import getpass,subprocess,glob
from .jobs import getBatchVariable,JOBCLASS
from .slurmFlags import sbatchFlags

    
########################################################################################################
#   SLURM classes/functions
########################################################################################################

class SLURMjob(object):
    
    def __init__(self,verbose=False):
        self.manager = "SLURM"                
        # Get job name and ID and identify whether job array
        self.jobName = getBatchVariable("SLURM_JOB_NAME",verbose=verbose,manager=self.manager)
        if self.jobName is None or self.jobName == "sh":
            self.interactive = True
        else:
            self.interactive = False
        try:
            jobID = os.environ["SLURM_ARRAY_JOB_ID"]
        except KeyError:
            self.jobID = getBatchVariable("SLURM_JOB_ID",verbose=verbose,manager=self.manager)
            self.jobArray = False
        else:
            jobID = getBatchVariable("SLURM_ARRAY_JOB_ID",verbose=verbose,manager=self.manager)
            self.jobArray = True
        self.account = getBatchVariable("SLURM_JOB_ACCOUNT",verbose=verbose,manager=self.manager)
        # Get user ID
        self.userID = getBatchVariable("SLURM_JOB_USER",verbose=verbose,manager=self.manager)
        # Get machine and queue
        self.machine = getBatchVariable("SLURM_CLUSTER_NAME",verbose=verbose,manager=self.manager)
        self.queue = getBatchVariable("SLURM_JOB_PARTITION",verbose=verbose,manager=self.manager)
        # Get submission dir
        self.submitDir = getBatchVariable("SLURM_SUBMIT_DIR",verbose=verbose,manager=self.manager)
        self.workDir = self.submitDir
        # Store variables for job array
        if self.jobArray:
            self.jobArrayID = getBatchVariable("SLURM_ARRAY_TASK_ID",verbose=verbose,manager=self.manager)
            self.taskID = self.jobArrayID
            self.minJobArrayID = getBatchVariable("SLURM_ARRAY_TASK_MIN",verbose=verbose,manager=self.manager)
            self.minTaskID = self.minJobArrayID
            self.maxJobArrayID = getBatchVariable("SLURM_ARRAY_TASK_MAX",verbose=verbose,manager=self.manager)
            self.maxTaskID = self.minJobArrayID
        else:
            self.jobArrayID = None
            self.taskID = None
            self.minJobArrayID = None
            self.maxJobArrayID = None
            self.minTaskID = None
            self.maxTaskID = None
        # Get nodes information
        self.nodes = getBatchVariable("SLURM_JOB_NUM_NODES",verbose=verbose,manager=self.manager)
        self.ppn = getBatchVariable("SLURM_JOB_CPUS_PER_NODE",verbose=verbose,manager=self.manager)
        if self.nodes is not None:
            self.nodes = int(self.nodes)
        if self.ppn is not None:                        
            if not self.ppn.isdigit():
                self.ppn = self.ppn.split("(")[0]
            self.ppn = int(self.ppn)
        if self.nodes is not None and self.ppn is not None:            
            self.cpus = self.nodes*self.ppn
        else:
            self.cpus = None
        return




class submitSLURM(JOBCLASS):
    
    def __init__(self,verbose=False,overwrite=False):
        super(submitSLURM, self).__init__("SLURM",verbose=verbose)
        self.cmd = "sbatch "
        self.appendable = True
        self.overwrite = overwrite
        return

    def canAppend(self):
        if not self.appendable:
            print("SLURM submission string is not appendable!")
        return self.appendable

    def replaceOption(self,old,new):
        S = re.search(old,self.cmd)
        if S:
            self.cmd = self.cmd.replace(old,new)
        return

    def checkForFlag(self,flag=None,longName=None):
        if flag is None and longName is None:
            return None
        S = None
        if flag is not None:
            S = re.search(' -'+flag+' (\w+) ',self.cmd)
            if S is not None: 
                return S
        if longName is not None:
            S = re.search(' --'+longName+'=(\w+) ',self.cmd)
            if S is not None: 
                return S
        return S

    def getFlag(self,flagName):
        if flagName in sbatchFlags.keys():
            flag = sbatchFlags[flagname]
        else:
            flag = None
        S = self.checkForFlag(flag=flag,longName=flagName)
        value = None
        if S is not None:
            value = S.group(1)
        return value
            

    def addFlag(self,flagName,value):
        if value is None: return
        if flagName in sbatchFlags.keys():
            flag = sbatchFlags[flagName]
        else:
            flag = None
        S = self.checkForFlag(flag=flag,longName=flagName)
        if S:
            if not self.overwrite:
                print("Flag '"+flagName+"' already specified with value = "+S.group(1))
                return
            else:
                self.replaceOption(S.group(0)," --"+flagName+"="+str(value))
        else:
            if not self.canAppend(): return
            self.cmd = self.cmd + " --"+flagName+"="+str(value)+" "
        return

    def specifyJobArray(self,arrayIndexes):        
        if arrayIndexes is None:
            return
        if type(runs) is list:
            value = ",".join(list(map(str,arrayIndexes)))
        else:
            value = arrayIndexes
        self.addFlag("array",value)
        return

    def specifyLogFiles(self,logdir,filePrefix=None,joinOE=True):        
        if logdir is not None:
            subprocess.call(["mkdir","-p",logdir])
        else:
            logdir = "."
        if not logdir.endswith("/"):
            logdir = logdir + "/"
        if filePrefix is None:
            name = self.getFlag('job-name')
            if name is None:
                filePrefix = "slurmJob"
            else:
                filePrefix = name
        outFile = logdir + filePrefix + '-%J.out'
        outFile.encode()
        errFile = logdir + filePrefix + '-%J.err'
        errFile.encode()
        if joinOE:
            self.addFlag("input",outFile)
        else:
            self.addFlag("output",outFile)
            self.addFlag("error",errFile)
        return

    def passScriptArguments(self,args):
        if args is None: return
        S = self.checkForFlag(longName="export")
        if S is not None:
            existingArgs = {}
            for obj in S.group(1).split(","):
                existingArgs[obj.split("=")[0]] = obj.split("=")[1]
            allKeys = list(set(existingArgs.keys()+args.keys()))
            allArgs = [] 
            for key in allKeys:
                if key in args.keys() and key in existingArgs.keys():
                    if self.overwrite:
                        thisArg = key+"="+str(args[key])
                    else:
                        thisArg = key+"="+str(existingArgs[key])
                else:
                    if key in args.keys():
                        thisArg = key+"="+str(args[key])
                    if key in existingArgs.keys():
                        thisArg = key+"="+str(existingArgs[key])
                allArgs.append(thisArg)
            argString = ",".join(allArgs)
        S = self.checkForFlag(longName="export") 
        if S is not None:
            self.replaceOption(S.group(0),"--export="+argString)
        else:
            self.addFlag(self,"export",argString)
        return

    def setScript(self,script):
        if script is None: return
        if not self.canAppend(): return
        self.appendable = False
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
        
    def addPartition(self,part):
        self.addFlag("partition",part)
        return

    def addJobName(self,name):
        self.addFlag("job-name",name)
        return

    def addAccount(self,account):
        self.addFlag("account",account)
        return

    def addLicense(self,License):
        self.addFlag("licenses",License)
        return

    def addQOS(self,qos):
        self.addFlag("qos",qos)
        return

    def addWorkdir(self,workdir):
        self.addFlag("workdir",workdir)
        return

    def addWalltime(self,walltime):
        self.addFlag("time",walltime)
        return

    def addNodes(self,nodes=1):
        if nodes is None:
            nodes = 1
        self.addFlag("nodes",str(nodes))

    def addCpusPerTask(self,ncpus):
        self.addFlag("cpus-per-task",str(ncpus))
        return
    
    def addTasksPerNode(self,ntasks):
        self.addFlag("cpus-per-node",str(ntasks))
        return

    def addTasksPerCore(self,ntasks):
        self.addFlag("cpus-per-core",str(ntasks))
        return

    def addConstraint(self,constraint):
        self.addFlag("constraint",constraint)
        return
    
    def addNodeType(self,nodeType):
        self.addConstraint(nodeType)
        return
