# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 09:39:36 2024

@author: Brent Thompson
"""

import os
import pandas as pd
import idp.file_management as fm
import database_manipulation as dm


#NEW_DATA = 'C:/UCF/FSEC/Data/New_Data/Dark_IV/'
NEW_DATA = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/Instrument_Data/Dark_IV_Data'
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'

# database_file_path = 'C:/UCF/FSEC/Data/Databases/
database_file_path = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/dark-iv-metadata.txt'

def parse_darkiv_metadata(folder_list):
    dark_IV_metadata = [
        'filename', 'date', 'time', 'make', 'model', 'serial-number', 'comment'
    ]
    dark_iv_filepaths = []
    stripped_folder = []

    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(".txt"):
                    file = os.path.join(dirpath, filename)
                    dark_iv_filepaths.append(file)

    print(f'{len(dark_iv_filepaths)} new files found for Dark IV')

    for file in dark_iv_filepaths:
        try:
            metadata_dict = fm.get_filename_metadata(file, 'dark_iv')

            filename = file.split('\\')[-1]

            date = metadata_dict.get('date')
            time = metadata_dict.get('time')
            make = metadata_dict.get('make')
            model = metadata_dict.get('model')
            serial = metadata_dict.get('serial_number')
            comment = metadata_dict.get('comment')

            metadata = [filename, date, time, make, model, serial, comment]
            stripped_folder.append(metadata)

        except ValueError:
            print(f'{file} failed to be processed, please review')
            continue

    Dark_IV_Metadata = pd.DataFrame(stripped_folder, columns=dark_IV_metadata)

    modules = pd.read_csv(
        MODULES, sep='\t', usecols=["module-id", "make", "model", "serial-number"])
    Dark_IV_Metadata = Dark_IV_Metadata.merge(
        modules, how='left', left_on="serial-number", right_on="serial-number")

    dark_iv_order = [
        'date', 'time', 'module-id', 'make', 'model', 'serial-number', 'comment', 'filename']
    Dark_IV_Metadata = Dark_IV_Metadata.drop(columns={'make_y', 'model_y'})
    Dark_IV_Metadata = Dark_IV_Metadata.rename(
        columns={'make_x': 'make', 'model_x': 'model'})
    Dark_IV_Metadata = Dark_IV_Metadata[dark_iv_order]
    #Dark_IV_Metadata.insert(0, column='ID', value='')

    return Dark_IV_Metadata


def dark_iv_database_updater():
    # Load in old dark_iv database to get the last date
    old_dark_iv = dm.read_database(database_file_path).sort_values('date')
    last_date = old_dark_iv['date'].iloc[-1]

# Create list of folders to run stripper function on
    folder_list = fm.search_folders(last_date, NEW_DATA)

# Create new dataframe with new files
    new_dark_iv = parse_darkiv_metadata(folder_list)

# Concat the two together, drop duplicates in case any were made
    updated_dark_iv_database = pd.concat(
        [old_dark_iv, new_dark_iv]).reset_index().drop(columns='index')
    final_dark_iv = updated_dark_iv_database.drop_duplicates(subset=[
                                                             'filename'])

# Save the dataframe
    dm.save_database(final_dark_iv, database_file_path)

    print('dark_iv_updated')
    return new_dark_iv


if __name__ == "__main__":
    dark_iv_database_updater()
