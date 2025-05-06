# This script was created by Joseph R and Albert J to scrape metadata from the image 
# database and create the appropriate tables based off of command line arguments.
# This script uses similar terminiology to Dylan Colvin's uvf_outdoor_database_updater script 
# for consistency and for future debuggers.



import csv
from PIL import Image 
import exifread
from PIL.ExifTags import TAGS
import argparse
import os
import pandas as pd
import database_manipulation as dm


def find_uvf_outdoor_files(dir_data):
    files = []
    for dirpath, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            #files.append(f"{dirpath}/{filename}")
            files.append(os.path.join(dirpath, filename))

    return files


def check_database_exists(dir_uvf_outdoor_database):
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


def find_new_files(files, uvf_outdoor_database):
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


def get_metadata_from_new_files(files, existing_database):
    def read_settings(files) -> list:
        settings_list = []  # Contains a list of dictionaries of settings used for data columns
        for directory in files:
            settings_dict = {}
            settings_file_path = os.path.join(directory, 'settings.txt')
            try:
                with open(settings_file_path, 'r') as f:
                    content = f.read().strip()
                    parts = content.split()

                    if len(parts) >= 3:
                        exposure_time = parts[0]
                        aperture = parts[1]  
                        iso = parts[2]

                        iso_value = iso.replace('ISO', '')

                        settings_dict['exposure-time-(s)'] = exposure_time
                        settings_dict['aperture'] = aperture  
                        settings_dict['ISO'] = iso_value
                        settings_dict['flash-intensity-(%)'] = 'NA'
                    else:
                        settings_dict['exposure-time-(s)'] = 'NA'
                        settings_dict['aperture'] = 'NA'  
                        settings_dict['ISO'] = 'NA'
                        settings_dict['flash-intensity-(%)'] = 'NA'
            except FileNotFoundError:
                print(f"Settings file not found: {settings_file_path}")
            except Exception as e:
                print(f"Error reading settings file {settings_file_path}: {e}")
            
            settings_list.append(settings_dict)  # Append the settings_dict to the list
            
        return settings_list

    # Beginning of Scraping the filename
    settings = read_settings(files)  
    metadata_list = []

    for idx, file in enumerate(files):
        metadata_dict = {}
        bn_split = os.path.basename(file).split('_')

        if len(bn_split) == 1:
            date = 'NA'
            module_id = bn_split[0]
        elif len(bn_split) < 4:
            date = bn_split[0]
            module_id = bn_split[1]
        else:
            module_id = bn_split[3]
            date = bn_split[0]

        metadata_dict['module_id'] = module_id
        metadata_dict['camera-id'] = 'SONY-a7s'
        metadata_dict['flash-source-id'] = 'FLASHPOINT-ZOOM-Li-ON-R2-TTL'
        metadata_dict['filename'] = os.path.basename(file)  # Fixed to assign the full filename as a string
        metadata_dict['date'] = date
        metadata_dict['time'] = 'NA'

        # Access the settings based on the current index
        if idx < len(settings):
            settings_dict = settings[idx]
        else:
            settings_dict = {}

        metadata_dict['exposure-time-(s)'] = settings_dict.get('exposure-time-(s)', 'NA')
        metadata_dict['aperture'] = settings_dict.get('aperture', 'NA')
        metadata_dict['ISO'] = settings_dict.get('ISO', 'NA')
        metadata_dict['flash-intensity-(%)'] = settings_dict.get('flash-intensity-(%)', 'NA')

        exists = any(existing['filename'] == metadata_dict['filename'] for existing in existing_database)

        if not exists:
            metadata_list.append(metadata_dict)

    return metadata_list


def run():
    dir_ucf = f"E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/"
    dir_data = "DOE_FY2020_MultiscaleCharacterization/Data/Field_UVF"
    dir_database = f"{dir_ucf}/General/FSEC_PVMCF/module_databases/"
    dir_module_database = f"{dir_database}/module-metadata.txt"
    dir_uvf_outdoor_database = f"{dir_database}/uvf-outdoor-metadata.txt"

    module_database = dm.read_database(dir_module_database)

    # Load all files
    files = find_uvf_outdoor_files(dir_data)
    # Check if database exists, initiate if it does not
    uvf_outdoor_database = check_database_exists(dir_uvf_outdoor_database)
    # Using loaded uvf_outdoor database, check for new files
    files = find_new_files(files, uvf_outdoor_database)
    # Update database with data from new files
    uvf_outdoor_database = get_metadata_from_new_files(files, uvf_outdoor_database)
    # Update database files
    dm.save_database(uvf_outdoor_database, dir_uvf_outdoor_database)


if __name__ == "__main__":
    run()