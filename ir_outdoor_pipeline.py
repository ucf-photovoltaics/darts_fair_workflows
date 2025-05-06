# This script was created by Joseph R and Albert J to scrape metadata from the image 
# database and create the appropriate tables based off of command line arguments.
# This script uses similar terminiology to Dylan Colvin's ir_outdoor_database_updater script 
# for consistency and for future debuggers.



import csv
from PIL import Image 
import exifread
from PIL.ExifTags import TAGS
import argparse
import os
import pandas as pd
import database_manipulation as dm


def find_ir_outdoor_files(dir_data):
    files = []
    for dirpath, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            #files.append(f"{dirpath}/{filename}")
            files.append(os.path.join(dirpath, filename))

    return files


def check_database_exists(dir_ir_outdoor_database):
    # initiate database, read if it already exists
    # pull first row and put them in pandas
    if os.path.isfile(database_path):
        #database = dm.read_database(database)
        database = load_existing_database(database_path)
    return database

#Loading existing_database (Update based on need)
def load_existing_database(database_path):
    if os.path.isfile(database_path):
        with open(database_path, 'r') as file:
            reader = csv.DictReader(file)
            return list(reader)
    return [] # Returns an empty list if the database doesn't exist


def find_new_files(files, ir_outdoor_database):
    # Checking between database and files using datetimes
    # Whatever files have not been added will remain in files list
    date_times = ['_'.join([str(d), str(t)])
                  for d, t in zip(database.date, database.time)]
    for date_time in date_times:
        for file in files:
            # if the database contains this file, skip to next date_time
            # Remove the file from the list to reduce search time
            if date_time in file:
                files.remove(file)
                break
    return files

def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def get_metadata_from_new_files(files, existing_database) ->list:
    metadata_list = [] #Contains all metadata dictionaries for each file

    for file in files:
        metadata_dict = {}
        bn_split = os.path.basename(file).split('_')
        ext = file.split('.')[-1]

        metadata_dict['camera-id'] = 'FLIR-DUO-PRO-R'
        metadata_dict['filename'] = os.path.basename(file)

        metadata_dict['date'] = bn_split[0]

        if len(bn_split) < 4:
            metadata_dict['module_id'] = None
        else:
            #Determine the module ID based on the filename structure
            if bn_split[2] in ['FGCU', 'VCAD', 'PVL', 'VOLTAGEOFF']:
                metadata_dict['module_id'] = bn_split[3]
            elif bn_split[2] == 'FSEC':
                metadata_dict['module_id'] = bn_split[4]
            elif bn_split[1] == 'FSEC':
                metadata_dict['module_id'] = bn_split[3]
            else:
                metadata_dict['module_id'] = bn_split[2]
        
        metadata_dict['time'] = bn_split[1] if is_number(bn_split[1]) else 'NA'
        metadata_dict['injection-time-(s)'] = None

        #Check if the metadata already exists in the existing database for the current module type
        exists = any(existing['filename'] == metadata_dict['filename'] for existing in existing_database)

        if not exists:
            #Append the extracted metadata dictionary to the list
            metadata_list.append(metadata_dict)
    return metadata_list


def run():
    dir_ucf = f"E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/"
    dir_data = f"{dir_ucf}Instrument_Data/IR_FLIR/"
    dir_database = f"{dir_ucf}/General/FSEC_PVMCF/module_databases/"
    dir_module_database = f"{dir_database}/module-metadata.txt"
    dir_ir_outdoor_database = f"{dir_database}/ir-outdoor-metadata.txt"

    module_database = dm.read_database(dir_module_database)

    # Load all files
    files = find_ir_outdoor_files(dir_data)
    # Check if database exists, initiate if it does not
    ir_outdoor_database = check_database_exists(dir_ir_outdoor_database)
    # Using loaded ir_outdoor database, check for new files
    files = find_new_files(files, ir_outdoor_database)
    # Update database with data from new files
    ir_outdoor_database = get_metadata_from_new_files(files, ir_outdoor_database, module_database)
    # Update database files
    dm.save_database(ir_outdoor_database, dir_ir_outdoor_database)


if __name__ == "__main__":
    run()