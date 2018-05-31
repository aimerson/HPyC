#! /usr/bin/env python

import sys,os,fnmatch,re
import numpy as np
import getpass,subprocess,glob
from .batchJobs import getBatchVariable,JOBCLASS,NULLjob

    
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



def submitSLURM(script,args=None,PARTITION=None,QOS=None,WALLTIME=None,JOBNAME=None,LOGDIR=None,RUNS=None,NODETYPE=None,\
                    NODES=1,TASKS=None,CPUS=None,ACCOUNT=None,WORKDIR=None,LICENSE=None,mergeOE=False,verbose=False,submit=True):
    import sys,os,getpass,fnmatch,subprocess,glob
    sjob = "sbatch "
    if PARTITION is not None:
        sjob = sjob + " -p "+PARTITION
    if QOS is not None:
        sjob = sjob + " --qos="+QOS
    if WALLTIME is not None:
        sjob = sjob + " --time " + WALLTIME
    if JOBNAME is not None:
        sjob = sjob + " -J "+JOBNAME
    if RUNS is not None:
        if type(RUNS) is list:
            RUNS = ",".join(map(str,RUNS))
        sjob = sjob + " --array="+str(RUNS)
    if LOGDIR is not None:
        subprocess.call(["mkdir","-p",LOGDIR])        
        if JOBNAME is None:
            filename = 'slurm'
        else:
            filename = JOBNAME   
        filename = filename +'-%J'
        out = LOGDIR+"/"+filename+'.out'
        out.encode()
        err = LOGDIR+"/"+filename+'.err'
        err.encode()        
        joint = LOGDIR+"/"+filename+'.out'
        joint.encode()
        if mergeOE:
            sjob = sjob + " -i "+joint
        else:
            sjob = sjob + " --output "+out + " --error "+ err
    if ACCOUNT is not None:
        sjob = sjob + "-A " + ACCOUNT
    if NODES is None:
        NODES = 1
    sjob = sjob + " -N " + str(NODES)
    if NODETYPE is not None:
        sjob = sjob + " -C "+NODETYPE
    if TASKS is not None:
        sjob = sjob + " --ntasks-per-node="+str(TASKS)
    if CPUS is not None:
        sjob = sjob + " --cpus-per-task="+str(CPUS)
    if WORKDIR is not None:
        sjob = sjob + " --workdir="+WORKDIR
    if LICENSE is not None:
        sjob = sjob + " -L "+LICENSE
    if args is not None:
        argStr = ",".join([k+"="+args[k] for k in args.keys()])
        sjob = sjob + " --export="+argStr
    sjob = sjob + " "+script
    if verbose:
        print(" Submitting SLURM job: "+sjob)
    if submit:
        os.system(sjob)
    return

