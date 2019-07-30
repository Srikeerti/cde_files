#!/usr/bin/env python
# coding: utf-8

# In[2]:


#######################################################################
# Project         : Well Being Management - Clinical Data Extract
# Client          : Health Care Service Corporation (HCSC)
# Author(s)       : HCSC CDE Offshore team Hyderabad
# Script function : Edge Node to HDFS file movement
#                   for LIVONGO,OMADA,HINGE HEALTH,OVIA,TIVITY
# Last modified on : 29th July 2019
#######################################################################


# In[3]:


# # Importing dependencies
import json
import ConfigParser
import csv
import os
import glob
import shutil
from collections import OrderedDict
import sys
from subprocess import call
import subprocess


# In[4]:


class wbm_ingest():
    
    def __init__(self,config_file_path,run_env):
        self.config_file_path = config_file_path
        self.run_env = run_env.lower()
        
        # Create ConfigParser object
        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file_path)
        
    def print_params(self):
        print self.config_file_path
        print self.run_env
        
    def resolve_paths(self):
        # Special case when env is Test
        if self.run_env == 'test':
            lcl_path = 'tst'
            hdfs_path = 'test'
        else:
        # Assigning both local,HDFS to same variable
            lcl_path = hdfs_path = self.run_env
        
        # LIVONGO path resolution
        self.liv_local_path = self.config.get('LIVONGO','LOCAL_PATH').replace('{$edge_env}',lcl_path)
        self.liv_archive = self.config.get('LIVONGO','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
        self.liv_wkly_enrol_hdfs_path = self.config.get('LIVONGO','WEEKLY_ENROL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.liv_wkly_bp_hdfs_path = self.config.get('LIVONGO','WEEKLY_BP_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.liv_wkly_bg_hdfs_path = self.config.get('LIVONGO','WEEKLY_BG_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

        # OMADA path resolution
        self.omada_local_path = self.config.get('OMADA','LOCAL_PATH').replace('{$edge_env}',lcl_path)
        self.omada_archive = self.config.get('OMADA','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
        self.omada_enroll_hdfs_path = self.config.get('OMADA','ENROLD_MEM_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.omada_clinical_hdfs_path = self.config.get('OMADA','CLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.omada_nclinical_hdfs_path = self.config.get('OMADA','NCLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

        # HINGE HEALTH path resolution
        self.hinge_local_path = self.config.get('HINGEHEALTH','LOCAL_PATH').replace('{$edge_env}',lcl_path)
        self.hinge_archive = self.config.get('HINGEHEALTH','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
        self.hinge_enroll_hdfs_path = self.config.get('HINGEHEALTH','ENROLL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.hinge_clinical_hdfs_path = self.config.get('HINGEHEALTH','ENG_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

        # OVIA path resolution
        self.ovia_local_path = self.config.get('OVIA','LOCAL_PATH').replace('{$edge_env}',lcl_path)
        self.ovia_archive = self.config.get('OVIA','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
        self.ovia_clinical_hdfs_path = self.config.get('OVIA','CLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
        self.ovia_enroll_hdfs_path = self.config.get('OVIA','ENROLL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

        # TIVITY path resolution
        self.tvty_local_path = self.config.get('TIVITY','LOCAL_PATH').replace('{$edge_env}',lcl_path)
        self.tvty_archive = self.config.get('TIVITY','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
        self.tvty_hdfs_path = self.config.get('TIVITY','HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

    def move_to_hdfs(self,source,local_path,hdfs_paths,file_patterns,extract_range,archive_path):

        print source
        print local_path
        print hdfs_paths
        print file_patterns
        print extract_range
        print archive_path
        
        # Delete status file if already exists
        # Modify the path accordingly in different
        # environments accordingly
        if os.path.isfile(self.config.get('TEST','WINDOWS_PATH')+'wbm_partition_info.csv'):
            os.remove(self.config.get('TEST','WINDOWS_PATH')+'wbm_partition_info.csv')
            print "Daily stats file removed"
        else:
            print "File does not exist"
            print "Nothing to check"
        
        # Zip and interate over hdfs path, file pattern and extract range
        for hdfs_path,file_pattern,extract in zip(hdfs_paths,file_patterns,extract_range):
        
            # Handling for JSON files
            if file_pattern.endswith('.json'):
                print "This a JSON file "
                print glob.glob(local_path+file_pattern)

                # Picking only latest file using timestamp            
                orig_json = os.path.basename(max(glob.glob(local_path+file_pattern),key=os.path.getmtime))

                print "Call JSON to CSV conversion routine"
                # JSON to CSV function call
                # PICK the CSV file which is returned 
                # by JSON to CSV function
                file_to_pick = os.path.basename(self.json_to_csv(source,max(glob.glob(local_path+file_pattern),
                                                                key=os.path.getmtime)))

                print "Returned with CSV filename as "+file_to_pick

            # Handling for non json files
            else:

                print "Non OVIA sources"
                # Pick latest file using timestamp
                file_to_pick = os.path.basename(
                max(glob.glob(local_path+file_pattern),key=os.path.getmtime))
            
            file_dt = file_to_pick[extract[0]:extract[1]].replace('_','')
        
            print "File to pick is "+file_to_pick
            print "File Date is "+file_dt
            print "Going to check for directory"
            print hdfs_path+file_dt
            complete_hdfs_path=hdfs_path+file_dt
            print("compelete path : " + complete_hdfs_path)
            # Check if partition already exists.WINDOWS
            # Do the same for HDFS directory check in LINUX
            proc = subprocess.Popen(['hdfs', 'dfs','-test','-e', complete_hdfs_path])
            proc.communicate()

            if proc.returncode != 0:


                print "This directory does not exist"
                #os.mkdir(hdfs_path+file_dt)
                #call(['hadoop', 'dfs', '-mkdir','{}'.format(hdfs_path+file_dt)])
		call(['hadoop', 'dfs', '-mkdir',hdfs_path+file_dt])
                
                #dir_status = os.path.isdir(hdfs_path+file_dt) 
                print "The directory is now created with status "
                #print dir_status
            else:

                print "Directory exists.No need to create it"
                #dir_status = os.path.isdir(hdfs_path+file_dt) 
                print "The directory already exists with status "
                #print dir_status
                #call(['hadoop', 'dfs', '-copyFromLocal','{}'.format(local_path+file_to_pick), '{}'.format(complete_hdfs_path)])
                call(['hdfs', 'dfs', '-copyFromLocal',local_path+file_to_pick, hdfs_path+file_dt])
    def source_wise_call(self,source):
        if source.upper() == 'LIVONGO':
            self.move_to_hdfs('LIVONGO',
                              self.liv_local_path,
                              [self.liv_wkly_enrol_hdfs_path,
                               self.liv_wkly_bp_hdfs_path,
                               self.liv_wkly_bg_hdfs_path],
                              [self.config.get('LIVONGO','ENROLL_FILE_PATTERN'),
                               self.config.get('LIVONGO','BLOOD_PRESSURE_FILE_PATTERN'),
                               self.config.get('LIVONGO','BLOOD_GLUCOSE_FILE_PATTERN')],
                              [(26,-6),(18,-6),(18,-6)],
                              self.liv_archive
                             )
        elif source.upper() == 'OVIA':
            self.move_to_hdfs('OVIA',
                              self.ovia_local_path,
                              [self.ovia_clinical_hdfs_path,self.ovia_enroll_hdfs_path],
                              [self.config.get('OVIA','CLINICAL_FILE_PATTERN'),
                               self.config.get('OVIA','ENROLL_FILE_PATTERN')],
                              [(36,-13),(38,-13)],
                              self.ovia_archive)

# In[5]:


# Create class object
wbm = wbm_ingest('/home/cloudera/Desktop/wbm_params_win.prm','dev')
wbm.print_params()
wbm.resolve_paths()


# In[6]:


wbm.source_wise_call('LIVONGO')


# In[ ]:


#wbm.source_wise_call('OVIA')

