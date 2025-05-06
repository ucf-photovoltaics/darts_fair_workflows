# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 10:10:49 2024

@author: dcolvin
"""

import pandas as pd
import os
import numpy as np

import database_manipulation as dm
import file_management as fm


def find_v10_files(dir_data):
    # Find all files in V10 folder
    files = []
    for dirpath, dirnames, filenames in os.walk(dir_data):
        for filename in filenames:
            files.append(f"{dirpath}/{filename}")

    return files


def check_database_exists(dir_v10_database):
    # initiate database, read if it already exists
    v10_database = pd.DataFrame(columns=['module-id', 'date', 'time', 'voltage-average-(V)',
                                         'current-average-(A)', 'temperature-average-(C)',
                                         'number-of-points', 'delay-time-(s)',
                                         'setpoint-total-time-(s)'])
    if os.path.isfile(dir_v10_database):
        v10_database = dm.read_database(dir_v10_database)

    return v10_database


def find_new_files(files, v10_database):
    # Checking between database and files using datetimes
    # Whatever files have not been added will remain in files list
    date_times = ['_'.join([str(d), str(t)])
                  for d, t in zip(v10_database.date, v10_database.time)]
    for date_time in date_times:
        for file in files:
            # if the database contains this file, skip to next date_time
            # Remove the file from the list to reduce search time
            if date_time in file:
                files.remove(file)
                break

    return files


def get_metadata_from_new_files(files, v10_database, module_database):
    # getting metadata from filenames and data
    for file_number, file in enumerate(files):
        metadata_dict = fm.get_filename_metadata(file, datatype='v10')

        # Use module-id, not serial number
        serial_number = metadata_dict['serial-number']
        # unknown_id = metadata_dict['serial-number']
        '''
        if (unknown_id[0] in ['F','P','V']) and (len(unknown_id) < 11):
            module_id = unknown_id
            serial_number = module_database[module_database['module-id'] == unknown_id]['serial-number'].values[0]
        else:
            module_id = module_database[module_database['serial-number'] == unknown_id]['module-id'].values[0]
            serial_number = unknown_id
        '''
        if 'ASU' in file:
            bn_split = os.path.basename(file).split('_')
            temp = f"{bn_split[3][6:]}_{serial_number}"
            if 'GTB_' in file:
                temp = f"{bn_split[4]}_{bn_split[5]}"

            module_id = module_database[module_database['serial-number']
                                        == temp]['module-id'].values[0]

        else:
            module_id = module_database[module_database['serial-number']
                                        == serial_number]['module-id'].values[0]
        data = pd.read_csv(file, sep='\t', index_col=False)

        metadata_dict.pop('serial-number')
        metadata_dict['module-id'] = module_id
        # np.mean(data['Voltage'])
        metadata_dict['voltage-average-(V)'] = np.mean(data.iloc[:, 0])
        # np.mean(data['Current'])
        metadata_dict['current-average-(A)'] = np.mean(data.iloc[:, 1])
        # np.mean(data['Temperature'])
        metadata_dict['temperature-average-(C)'] = np.mean(data.iloc[:, 3])
        metadata_dict['number-of-points'] = len(data)

        append_index = len(v10_database)
        for key, value in metadata_dict.items():
            v10_database.loc[append_index, key] = str(value)

        # Correcting file formats and names
        # print(f"Finished file {i+1} of {len(files)}.")
        # columns_old = list(data.columns)
        # columns_new = [c.lower() for c in columns_old]
        # fix = dict(zip(columns_old,columns_new))
        # data.rename(columns=fix,inplace=True)
        # data.to_csv(file, header=True, index=None, sep='\t')

        #newname = file.replace(module_id, serial_number)
        # os.rename(file,newname)
    return v10_database


def parse_v10_metadata():
    dir_ucf = f"E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/"
    #dir_ucf = f"C:/Users/dcolvin/University of Central Florida/UCF_Photovoltaics_GRP - Documents/"
    dir_data = f"{dir_ucf}Instrument_Data/V10/"
    dir_database = f"{dir_ucf}/General/FSEC_PVMCF/module_databases/"
    dir_module_database = f"{dir_database}/module-metadata.txt"
    dir_v10_database = f"{dir_database}/v10-metadata.txt"

    module_database = dm.read_database(dir_module_database)

    # Load all files
    files = find_v10_files(dir_data)
    # Check if database exists, initiate if it does not
    v10_database = check_database_exists(dir_v10_database)
    # Using loaded v10 database, check for new files
    files = find_new_files(files, v10_database)
    # Update database with data from new files
    v10_database = get_metadata_from_new_files(
        files, v10_database, module_database)
    # Update database files
    dm.save_database(v10_database, dir_v10_database)


if __name__ == "__main__":
    parse_v10_metadata()
