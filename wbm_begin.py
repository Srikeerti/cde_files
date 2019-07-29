#!/usr/bin/env python
# coding: utf-8

# In[315]:


#######################################################################
# Project         : Well Being Management - Clinical Data Extract
# Client          : Health Care Service Corporation (HCSC)
# Author(s)       : HCSC CDE Offshore team Hyderabad
# Script function : Edge Node to HDFS file movement
#                   for LIVONGO,OMADA,HINGE HEALTH,OVIA,TIVITY
# Last modified on : 29th July 2019
#######################################################################


# In[316]:


# Importing dependencies
import json
import ConfigParser
import csv
import os
import glob
import shutil
from collections import OrderedDict
import sys


# In[37]:


usr_env = raw_input("Enter environment (dev,test,prod) : ").lower()

# Validate user input for environment
# Raise error for incorrect value
if usr_env in ['dev','test','prod']:
    print "Environment entered by user : "+usr_env
else:
    print "Enter any of dev,test,prod"
    raise ValueError
    


# In[128]:


# Create ConfigParser object
config = ConfigParser.ConfigParser()

# Read from param file
config.read('wbm_params_win.prm')


# In[317]:


# Resolving local,hdfs and archival paths 
# for all sources from config file

# Special case when env is Test
if usr_env == 'test':
    lcl_path = 'tst'
    hdfs_path = 'test'
else:
    # Assigning both local,HDFS to same variable
    lcl_path = hdfs_path = usr_env
    
# LIVONGO path resolution
liv_local_path = config.get('LIVONGO','LOCAL_PATH').replace('{$edge_env}',lcl_path)
liv_archive = config.get('LIVONGO','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
liv_wkly_enrol_hdfs_path = config.get('LIVONGO','WEEKLY_ENROL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
liv_wkly_bp_hdfs_path = config.get('LIVONGO','WEEKLY_BP_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
liv_wkly_bg_hdfs_path = config.get('LIVONGO','WEEKLY_BG_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)


# OMADA path resolution
omada_local_path = config.get('OMADA','LOCAL_PATH').replace('{$edge_env}',lcl_path)
omada_archive = config.get('OMADA','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
omada_enroll_hdfs_path = config.get('OMADA','ENROLD_MEM_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
omada_clinical_hdfs_path = config.get('OMADA','CLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
omada_nclinical_hdfs_path = config.get('OMADA','NCLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

# HINGE HEALTH path resolution
hinge_local_path = config.get('HINGEHEALTH','LOCAL_PATH').replace('{$edge_env}',lcl_path)
hinge_archive = config.get('HINGEHEALTH','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
hinge_enroll_hdfs_path = config.get('HINGEHEALTH','ENROLL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
hinge_clinical_hdfs_path = config.get('HINGEHEALTH','ENG_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
    
# OVIA path resolution
ovia_local_path = config.get('OVIA','LOCAL_PATH').replace('{$edge_env}',lcl_path)
ovia_archive = config.get('OVIA','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
ovia_clinical_hdfs_path = config.get('OVIA','CLINICAL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)
ovia_enroll_hdfs_path = config.get('OVIA','ENROLL_HDFS_PATH').replace('{$hdfs_env}',hdfs_path)

# TIVITY path resolution
tvty_local_path = config.get('TIVITY','LOCAL_PATH').replace('{$edge_env}',lcl_path)
tvty_archive = config.get('TIVITY','ARCHIVE_PATH').replace('{$edge_env}',lcl_path)
tvty_hdfs_path = config.get('TIVITY','HDFS_PATH').replace('{$hdfs_env}',hdfs_path)


# In[313]:


# This function is to convert JSON to CSV
# Takes source and JSON file as input
# Returns converted CSV with path to caller
def json_to_csv(source,file_to_convert):
    
    print "JSON to CSV conversion"
    # Begin CSV conversion for JSON file
    json_file = ''.join(file_to_convert)
    
    with open(json_file,'r') as f:
        data = json.load(f,object_pairs_hook=OrderedDict)
        print data
        
        # Choosing name for CSV file
        # Same as JSON except for extension
        csv_file = json_file.split('.')[0]+'.csv'
        
     # OVIA source handling   
    if source.lower() == 'ovia':
        # Extracting list of dictionaries
        # Specific to OVIA source
        ovia_data = data['report']['records']
        
        # Initialising count
        # for separating header and data
        count = 0
        # Looping over OVIA JSON data
        for json in ovia_data:
            if count == 0:
                print "Going to write CSV header"
                # Capture header as a list
                header = list(json.keys())
                # Open file in 'w' mode
                # to write header
                with open(csv_file,'w') as csv_head:
                    wr = csv.writer(csv_head,lineterminator='\n')
                    wr.writerow(header)
                # Increment count
                # This IF executes only once for JSON
                # for header
                count += 1
            
            print "Going to write CSV data"
            # Getting values as list
            values = list(json.values())
            print values
            # Write to same file as header
            # in 'a' mode
            with open(csv_file,'a') as csv_data:
                    wr = csv.writer(csv_data,lineterminator='\n')
                    wr.writerow(values)
        # Return CSV file with path
        return csv_file


# In[311]:


# This function moves local files to HDFS directories
# Generic function for all WBM sources
# Takes Source, Local Path, HDFS Paths, File Patterns, Date extract range
# and archival path as input
def move_to_hdfs(source,local_path,hdfs_paths,file_patterns,extract_range,archive_path):
    
    # Delete status file if already exists
    # Modify the path accordingly in different
    # environments accordingly
    if os.path.isfile(config.get('TEST','WINDOWS_PATH')+'wbm_partition_info.csv'):
        os.remove(config.get('TEST','WINDOWS_PATH')+'wbm_partition_info.csv')
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
            file_to_pick = os.path.basename(json_to_csv(source,max(glob.glob(local_path+file_pattern),
                                                            key=os.path.getmtime)))
            
            print "Returned with CSV filename as "+file_to_pick
        
        # Handling for non json files
        else:
            
            print "Non OVIA sources"
            # Pick latest file using timestamp
            file_to_pick = os.path.basename(
            max(glob.glob(local_path+file_pattern),key=os.path.getmtime))

        # Extract YYYYMM from date in file
        # extract[0] and extract[1] are provided 
        # as arguments to this function
        file_dt = file_to_pick[extract[0]:extract[1]].replace('_','')
        
        print "File to pick is "+file_to_pick
        print "File Date is "+file_dt
        print "Going to check for directory"
        print hdfs_path+file_dt
        
        # Check if partition already exists.WINDOWS
        # Do the same for HDFS directory check in LINUX
        if os.path.isdir(hdfs_path+file_dt) == False:
            print "This directory does not exist"
            os.mkdir(hdfs_path+file_dt)
            dir_status = os.path.isdir(hdfs_path+file_dt) 
            print "The directory is now created with status "
            print dir_status
        else:
            print "Directory exists.No need to create it"
            dir_status = os.path.isdir(hdfs_path+file_dt) 
            print "The directory already exists with status "
            print dir_status
            
        # HDFS movement
        if dir_status == True:
            print "Now moving"
            # Copy and archive
            # Write LINUX specific command
            shutil.copyfile(local_path+file_to_pick,hdfs_path+file_dt+"/"+file_to_pick)

            if os.path.isfile(hdfs_path+file_dt+"/"+file_to_pick):
                print "File move successful"
                print "Proceed for archival"
                # Archival
                if source.lower() == 'ovia':
                    print "OVIA archival"
                    print local_path+orig_json
                    print archive_path+orig_json
                    print "Just printed the paths above"
                    # Write LINUX specific command
                    shutil.move(local_path+orig_json,archive_path+orig_json)
                else: 
                    # Write LINUX specific command
                    shutil.move(local_path+file_to_pick,archive_path+file_to_pick)

            # Archival check
            if os.path.isfile(liv_archive+file_to_pick) and source.lower() not in ('ovia','hingehealth'):
                print "Archive successful for non json files"
                src_status = [local_path+file_to_pick,hdfs_path+file_dt+"/"+file_to_pick,'HDFS Moved','Archived']
            else: 
                print "Archive successful for non json files"
                src_status = [local_path+orig_json,hdfs_path+file_dt+"/"+file_to_pick,'HDFS Moved','Archived']
                # Write LINUX specific command
                os.remove(local_path+file_to_pick)
                print "Removed "+local_path+file_to_pick
            with open(config.get('TEST','WINDOWS_PATH')+'wbm_partition_info.csv','a+') as f:
                #writer = csv.writer('wbm_partition_info.csv',delimiter=',')
                writer = csv.writer(f,delimiter='|',quoting=csv.QUOTE_ALL,lineterminator='\n')
                writer.writerow(src_status)


# In[318]:


# LIVONGO HDFS movement and archival call
move_to_hdfs('LIVONGO',
             liv_local_path,
             [liv_wkly_enrol_hdfs_path,liv_wkly_bp_hdfs_path,liv_wkly_bg_hdfs_path],
            [config.get('LIVONGO','ENROLL_FILE_PATTERN'),config.get('LIVONGO','BLOOD_PRESSURE_FILE_PATTERN'),config.get('LIVONGO','BLOOD_GLUCOSE_FILE_PATTERN')],
            [(26,-6),(18,-6),(18,-6)],
             liv_archive)


# In[312]:


# OVIA HDFS movement and archival call
move_to_hdfs('OVIA',
             ovia_local_path,
             [ovia_clinical_hdfs_path,ovia_enroll_hdfs_path],
            [config.get('OVIA','CLINICAL_FILE_PATTERN'),config.get('OVIA','ENROLL_FILE_PATTERN')],
            [(36,-13),(38,-13)],
             ovia_archive)

