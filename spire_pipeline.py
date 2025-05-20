# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 15:23:52 2025

@author: Brent Thompson
"""
import os
import pandas as pd
import sqlite3


# Collect spire file folders, Not used but may be useful
def get_spire_folders(path):
    folders = []
    for dirpath, dirname, filename in os.walk(path):
        date = dirpath.split("/")[-1]
        if len(date) == 6: # 6 is YYMMDD
            folders.append(dirpath)
    return folders


# Collect all of one type of file to be processed, ie CSV or TXT
def get_files_from_folders(path='', filetype='txt', filename_only=False):
    
    filetype_folder = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith(f".{filetype}"):
                if filename_only:
                    filetype_folder.append(filename.split('.')[0])
                else:
                    file = os.path.join(dirpath, filename)
                    filetype_folder.append(file)
    return filetype_folder 

# Transform the spire CSV to a dataframe with slight transforms, add filename
def parse_spire_metadata(file):
    spire_dataframe = pd.DataFrame(pd.read_csv(file)).T
    spire_metadata = spire_dataframe.iloc[:, :80]
    
    spire_metadata.iat[0, 9] = "Intesity Corrected TO:"
    spire_metadata.iat[0, 79] = "filename"
    
    file_iri = file.split("/")[-1]
    spire_metadata.iat[1, 79] = file_iri

    spire_metadata.columns = spire_metadata.iloc[0].drop_duplicates()
    spire_metadata = spire_metadata.drop(spire_metadata.index[0])
    
    return spire_metadata


# Use Sqlite database table as storage for new tables
def add_record_to_database(spire_metadata):
    spire_metadata.to_sql('spire', conn, if_exists='append', index=True)
    return 'Success'




"""
USER INPUT 

spire_data_path should contain folders for each date of measurements,

database_path should also be a local file location. A database will be created.

"""

spire_data_path = "C:/localmachine/Spire/Data/"

database_path = "C:/localmachine/desired/database/location"



failed_files = [] # Catches outliers for further investigation
spire_files = get_files_from_folders(spire_data_path, filetype='csv')
conn = sqlite3.connect(f'{database_path}/spire_database.db') # Create DB

for index, file in enumerate(spire_files):
    try:
        spire_metadata = parse_spire_metadata(file)
        add_record_to_database(spire_metadata)
        print(index)
    except:
        failed_files.append(file)
        print(file + "failed")
        
# End result will be a sqlite datebase table at the database_path
        

