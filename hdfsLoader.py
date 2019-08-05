#!/usr/bin/env python
# coding: utf-8

# In[1]:


# HDFS loader
# Takes LFS source and HDFS destination as arguments


# In[11]:


# Importing dependencies
import subprocess


# In[10]:


def hdfsLoader(lfs_src,hdfs_dest):
    print "Into HDFS loader"
    print "Source is "+lfs_src
    print "Destination is "+hdfs_dest
    
    try:
        print "Test if HDFS path exists"
        print "Directory check status "+subprocess.check_output(["hdfs","dfs","-test","-e",hdfs_dest])
        
        try:
            print "HDFS move on success:\n" +subprocess.check_output(["hdfs", "dfs", "-put",
                                                                           lfs_src,hdfs_dest])
        except subprocess.CalledProcessError as e:
            print "HDFS move on error:\n" + e.output
            
    except subprocess.CalledProcessError as err:
        print "Test HDFS path error "+err.output

