# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 10:36:50 2024
@author: Brent Thompson

This function opens each individual mfr file and pulls the metadata values.

"""
import os
import pandas as pd
import idp.file_management as fm
import database_manipulation as dm

# Location of current database
database_file_path = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/sinton-iv-metadata.txt'
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'

#database_file_path = 'C:/UCF/FSEC/Data/Databases/sinton-iv-metadata.txt'
# Set the directory path for the script to run in
folder = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/Instrument_Data/Sinton_FMT/Results/MultiFlash'
#folder = 'C:/UCF/FSEC/Data/sinton'


def parse_mfr_metadata(folder_list):

    # List containing the names for each metadata field
    IV_metadata = [
        'mfr_filename', 'txt_filename', 'date', 'time', 'serial-number', 'load-voltage-(mV)',
        'reference-constant-(V/sun)', 'voltage-temperature-coefficient-(mV/C)',
        'temperature-offset-(C)', 'setpoint-initial-(mV/cell)',
        'step-size-one-(mV/cell)', 'step-size-switch-(mV/cell)',
        'step-size-two-(mV/cell)', 'setpoint-isc-voltage-(mV/cell)',
        'pulse-wait-time-(ms)', 'pulse-wait-time-voc-(ms)',
        'pulse-length-(us)', 'pulse-wait-time-voc-length-(us)']

# List containing keywords used to identify and select information
    lines_to_strip = (
        'Full IV', 'Flash Wait', 'Load', 'Reference', 'Voltage Temp',
        'Temperature Offset')


# Data structures to be used in following algorithm
    mfr_filepath = []  # Used to hold filepaths for open() method
    mfr_filename = []  # Names of the MFR files
    txt_filename = []  # Names of exsisting TXT files
    stripped_folder = []  # Holds lists of information during each iteration
    failed_files = []  # Holds any files that failed processing
    files_processed = 0  # Counter to display number files processed


# Algorithm to go through entire folder, and select each file
    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(".mfr"):
                    file = os.path.join(dirpath, filename)
                    mfr_filepath.append(file)
                    mfr_filename.append(filename)
                if filename.endswith(".txt"):
                    txt_filename.append(filename.split('.')[0])

    print(f'{len(mfr_filepath)} new files found ending in .MFR')
    print(f'{len(txt_filename)} new files found ending in .TXT')

# Algorithm to go through each mrf file, and extract relevent information
    for file in mfr_filepath:
        data_list = []  # Stores information during each loop
        files_processed += 1


# Open the file and extract nessasary infromation from filename and contents
        try:
            text_file = open(file, errors="ignore")

# Preform splits on basename to get date, time, serial-number
            basename = text_file.name.split('\\')[-1]
            if basename.split('.')[0].replace('IVT', '') in txt_filename:
                txt_file = basename.replace('mfr', 'txt')
            else:
                txt_file = "none"
            try:

                metadata_dict = fm.get_filename_metadata(file, datatype='iv')
            except:
                pass
            date = metadata_dict.get('date')
            time = metadata_dict.get('time')
            serial = metadata_dict.get('serial_number')

# Save this information in data_list
            metadata = [basename, txt_file, date, time, serial]
            data_list.extend(metadata)

# Read the contents of MFR file to extract metadata
            raw_text = text_file.readlines()

# Strip out unnessasary information from metadata ( = and " )
            for line in raw_text:
                if line.startswith(lines_to_strip):
                    line_value = line.split('= ')[1].replace('"', '').strip()
                    data_list.append(line_value)
            stripped_folder.append(data_list)
            text_file.close()

        except ValueError:
            failed_files.append(file)
            print(f'{file} failed to be processed, please review')
            continue

        if (files_processed % 10 == 0):
            print(f'{files_processed} IV curves processed so far...')


# Create dataframe for human readable database, save it as csv

    Sinton_IV_Metadata = pd.DataFrame(stripped_folder, columns=IV_metadata)
    
    
    modules = pd.read_csv(
        MODULES, sep='\t', usecols=["module-id", "make", "model", "serial-number"])
    Sinton_IV_Metadata = Sinton_IV_Metadata.merge(
        modules, how='left', left_on="serial-number", right_on="serial-number")

    mrf_order = [
        "date", "time", "module-id", "make", "model", "serial-number",
        "load-voltage-(mV)", "reference-constant-(V/sun)", "voltage-temperature-coefficient-(mV/C)",
        "temperature-offset-(C)", "setpoint-initial-(mV/cell)", "step-size-one-(mV/cell)",
        "step-size-switch-(mV/cell)", "step-size-two-(mV/cell)", "setpoint-isc-voltage-(mV/cell)",
        "pulse-wait-time-(ms)", "pulse-wait-time-voc-(ms)", "pulse-length-(us)",
        "pulse-wait-time-voc-length-(us)", "mfr_filename", "txt_filename"]

    Sinton_IV_Metadata = Sinton_IV_Metadata[mrf_order]

    dm.save_database(Sinton_IV_Metadata, database_file_path)

    return Sinton_IV_Metadata


def mfr_database_updater():
    # Load in old mfr database to get the last date
    old_mfr = dm.read_database(database_file_path)
    #old_mfr = old_mfr[:-1]
    
    last_date = old_mfr['date'].iloc[-1]

# Create list of folders to run stripper function on
    folder_list = fm.search_folders(last_date, folder)

# Create new dataframe with new files
    new_mfr = parse_mfr_metadata(folder_list)

# Concat the two together, drop duplicates in case any were made
    updated_mfr_database = pd.concat(
        [old_mfr, new_mfr]).reset_index().drop(columns='index')
    final_mfr = updated_mfr_database.drop_duplicates(subset=['mfr_filename'])

# Save the dataframe
    final_mfr.to_csv(database_file_path, index=False, mode='w')
    dm.save_database(final_mfr, database_file_path)

    print('mfr updated')
    return new_mfr


if __name__ == "__main__":
    mfr_database_updater()
