#!/usr/bin/env python
# coding: utf-8

# In[1]:


# This is the starter script
# for WBM ingestion
# Takes two arguments
# First path of config file
# Second filename


# In[17]:


# ConfigParser
import ConfigParser

# socket import for hostname
import socket

# For creation of run file
import csv


# In[3]:


# Take param file as first input
# Change this as sys.args[0]
param_file = raw_input("Enter parameter file path :: ")


# In[4]:


# Take source file name as second input
# Change this as sys.args[1]
source_file = raw_input("Enter source file name :: ")


# In[5]:


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


# In[6]:


# Resolving local,hdfs and archival paths 
# for all sources from config file
# Special case when env is Test
if usr_env == 'test':
    lcl_path = 'tst'
    hdfs_path = 'test'
else:
    # Assigning both local,HDFS to same variable
    lcl_path = hdfs_path = usr_env


# In[28]:


# ALter this portion with dynamic pickup
if any(['ovia' in source_file.lower(), 'clinical' in source_file.lower()]):
    print "Coming from OVIA source and clinical"
    src = 'OVIA'
    path_to_pick = 'CLINICAL_HDFS_PATH'
    
elif any(['ovia' in source_file.lower() and 'enrollment' in source_file.lower()]):
    print "Coming from OVIA source and clinical"
    src = 'OVIA'
    path_to_pick = 'ENROLL_HDFS_PATH'
    
# Add other source details here
else:
    print "No source found"


# In[15]:


# Create ConfigParser object
config = ConfigParser.ConfigParser()
# Read from param file
config.read(param_file)


# In[26]:


# Stats list
stats = [config.get(src,'LOCAL_PATH').replace('${edge_env}',lcl_path)+source_file,
         config.get(src,path_to_pick).replace('${hdfs_env}',hdfs_path),
        source_file]


# In[27]:


# Run file for subsequent processes
# hdfsLoader uses this information
with open(config.get('INFO','STATS_PATH')+"run_file.wbm",'w') as stats_file:
    writer = csv.writer(stats_file,delimiter='|',lineterminator='\n')
    writer.writerow(stats)

