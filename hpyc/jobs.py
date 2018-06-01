#! /usr/bin/env python

import sys,os,fnmatch,re
import numpy as np
import getpass,subprocess,glob


def getBatchVariable(variable,verbose=False,manager=None):
    if variable not in os.environ.keys():
        if manager is None:
            print("WARNING! Environment variable '"+variable+"' not found!")
        else:
            print("WARNING! "+manager+" environment variable '"+variable+"' not found!")
        value = None
    else:         
        value = os.environ[variable]
        if verbose:
            print(variable+" = "+str(value))                
    return value    
    

class JOBCLASS(object):    
    def __init__(self,manager,verbose=False):
        self.manager = manager
        self.verbose = verbose
        return

    def getEnvironmentVariable(self,variable):
        if variable not in os.environ.keys():
            if self.verbose:
                if self.manager is None:
                    print("WARNING! Environment variable '"+variable+"' not found!")
                else:
                    print("WARNING! "+self.manager+" environment variable '"+variable+"' not found!")
            value = None
        else:         
            value = os.environ[variable]
            if self.verbose:
                print(variable+" = "+str(value))                
        return value    

