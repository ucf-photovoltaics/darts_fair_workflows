# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 11:55:42 2024
@author: Brent
"""
import os
import exifread
import pandas as pd
import idp.file_management as fm
import database_manipulation as dm
#NEW_DATA = 'C:/UCF/FSEC/Data/New_Data/Dark_IV/'
NEW_DATA = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/Instrument_Data/IR_ICI'
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'
# database_file_path = 'C:/UCF/FSEC/Data/Databases/
database_file_path = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/ir-indoor-metadata.txt'
def parse_indoor_ir_metadata(folder_list):
    indoor_ir_metadata = [
    'date', 'time', 'make', 'model', 'serial-number', 'comment',
    'current', 'exposure_time', 'filename']
    indoor_ir_filepaths = []
    stripped_folder = []
    failed_files = []
    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(".jpg"):
                    file = os.path.join(dirpath, filename)
                    indoor_ir_filepaths.append(file)
    print(f'{len(indoor_ir_filepaths)} new files found for Indoor IR')
    for file in indoor_ir_filepaths:
        try:
            metadata_dict = fm.get_filename_metadata(file, 'ir')
            filename = file.split('\\')[-1]
            date = metadata_dict.get('date')
            time = metadata_dict.get('time')
            make = metadata_dict.get('make')
            model = metadata_dict.get('model')
            serial = metadata_dict.get('serial_number')
            comment = metadata_dict.get('comment')
            current = metadata_dict.get('current')
            
            exposure_time = metadata_dict.get('exposure_time')
            
            metadata = [date, time, make, model, serial, comment, current,
                        exposure_time, filename]
            
            stripped_folder.append(metadata)
        except ValueError as ve:
            print(f'{file} failed to be processed, please review. {ve}')
            failed_files.append(file)
            continue
        except AttributeError as ae:
            print(f'{file} failed to be processed, please review. {ae} ')
            failed_files.append(file)
            continue
        except IndexError as ie:
            print(f'{file} failed to be processed, please review. {ie} ')
            failed_files.append(file)
            continue
            
    Indoor_IR_Metadata = pd.DataFrame(stripped_folder, columns=indoor_ir_metadata)
    Indoor_IR_Metadata = dm.join_module_metadata(Indoor_IR_Metadata)
    
    ir_order = [
    'date', 'time', 'module-id', 'make', 'model', 'serial-number', 'comment',
    'current', 'exposure_time', 'filename']
    
    Indoor_IR_Metadata = Indoor_IR_Metadata[ir_order]
    #Indoor_IR_Metadata.insert(0, column='ID', value='')
    return Indoor_IR_Metadata
def indoor_ir_database_updater():
    # Load in old dark_iv database to get the last date
    old_ir = dm.read_database(database_file_path).sort_values('date')
    last_date = old_ir['date'].iloc[-1]
# Create list of folders to run stripper function on
    folder_list = fm.search_folders(last_date, NEW_DATA)
# Create new dataframe with new files
    new_ir = parse_indoor_ir_metadata(folder_list)
# Concat the two together, drop duplicates in case any were made
    updated_ir_database = pd.concat(
        [old_ir, new_ir]).reset_index().drop(columns='index')
    final_ir = updated_ir_database.drop_duplicates(subset=['filename'])
# Save the dataframe
    dm.save_database(final_ir, database_file_path)
    print('indoor_ir_updated')
    return new_ir
if __name__ == "__main__":
    indoor_ir_database_updater()