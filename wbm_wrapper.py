#!/usr/bin/env python
# coding: utf-8

# In[2]:


# WBM Ingestion wrapper
# Script to take param file and source file name as input


# In[88]:


# Importing dependencies
from hdfsLoader import hdfsLoader

# ConfigParser
import ConfigParser

# socket import for hostname
import socket


# In[6]:


# Take param file as first input
# Change this as sys.args[0]
param_file = raw_input("Enter parameter file path :: ")


# In[7]:


# Take source file name as second input
# Change this as sys.args[1]
source_file = raw_input("Enter source file name :: ")


# In[76]:


if 'ovia' in source_file.lower() and 'clinical' in source_file.lower():
    print "Coming from OVIA source and clinical"
    src = 'OVIA'
    path_to_pick = 'CLINICAL_HDFS_PATH'
    
elif 'ovia' in source_file.lower() and 'enrollment' in source_file.lower():
    print "Coming from OVIA source and clinical"
    src = 'OVIA'
    path_to_pick = 'ENROLL_HDFS_PATH'
# Add other source details here
else:
    print "No source found"


# In[61]:


# Create ConfigParser object
config = ConfigParser.ConfigParser()
# Read from param file
config.read(param_file)


# In[53]:


# Get the hostname and then decide on the env
# Make necessary changes here
if socket.gethostname().startswith("US"):
    print "Assigning dev"
    usr_env = 'dev'
elif socket.gethostname().startswith("IN"):
    print "Assigning test"
    usr_env = 'test'
elif socket.gethostname().startswith("UK"):
    print "Assigning prod"
    usr_env = 'prd'


# In[72]:


# Resolving local,hdfs and archival paths 
# for all sources from config file
# Special case when env is Test
if usr_env == 'test':
    lcl_path = 'tst'
    hdfs_path = 'test'
else:
    # Assigning both local,HDFS to same variable
    lcl_path = hdfs_path = usr_env


# In[80]:


# Deriving local path
local_src = config.get(src,'LOCAL_PATH').replace('{$edge_env}',lcl_path)+source_file
hdfs_dest = config.get(src,path_to_pick).replace('{$hdfs_env}',hdfs_path)


# In[86]:


# Calling hdfsLoader
hdfsLoader(local_src,hdfs_dest)

