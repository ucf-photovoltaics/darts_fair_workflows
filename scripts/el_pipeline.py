# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 11:56:44 2024

@author: Brent 
"""


import os
import exifread

import pandas as pd
import file_management as fm
import database_manipulation as dm


#NEW_DATA = 'C:/UCF/FSEC/Data/New_Data/Dark_IV/'
NEW_DATA = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/Instrument_Data/EL_DSLR_CMOS'
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'

# database_file_path = 'C:/UCF/FSEC/Data/Databases/
database_file_path = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/el-metadata.txt'

def extract_EXIF_data(image_file) -> dict:
    """Extract essential EXIF data from an image file."""
    exif_data = {}
    try:
        with open(image_file, 'rb') as f:
            tags = exifread.process_file(f)
        tags_needed = ['EXIF ExposureTime', 'EXIF ISOSpeedRatings', 'EXIF FNumber']
        for tag in tags_needed:
            exif_data[tag] = tags.get(tag, 'NA')
    except Exception as e:
        print(f"Error extracting EXIF data for {image_file}: {e}")
    return exif_data

def parse_el_metadata(folder_list):
    el_metadata = [
    'date', 'time', 'make', 'model', 'serial-number', 'comment',
    'exposure_time', 'current', 'voltage', 'iso', 'aperture','camera', 'filename']
    el_filepaths = []
    stripped_folder = []
    failed_files = []

    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(".jpg"):
                    file = os.path.join(dirpath, filename)
                    el_filepaths.append(file)

    print(f'{len(el_filepaths)} new files found for EL')
    for index, file in enumerate(el_filepaths):
        try:
            metadata_dict = fm.get_filename_metadata(file, 'el')
            exif_data = extract_EXIF_data(file)

            filename = file.split('\\')[-1]

            date = metadata_dict.get('date')
            time = metadata_dict.get('time')
            make = metadata_dict.get('make')
            model = metadata_dict.get('model')
            serial = metadata_dict.get('serial_number')
            comment = metadata_dict.get('comment')
            current = metadata_dict.get('current')
            voltage = metadata_dict.get('voltage')
            
            iso = exif_data.get('EXIF ISOSpeedRatings').printable
            aperture = exif_data.get('EXIF FNumber').printable
            exposure_time = exif_data.get('EXIF ExposureTime').printable
            camera = "DSLR-CMOS"
            
            metadata = [date, time, make, model, serial, comment, exposure_time,
                        current, voltage, iso, aperture, camera, filename]
            
            stripped_folder.append(metadata)
            print(f'{index} EL Images processed so far...')
        
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

        


    EL_Metadata = pd.DataFrame(stripped_folder, columns=el_metadata)

    EL_Metadata = dm.join_module_metadata(EL_Metadata)
    
    el_order = ['date', 'time', 'module-id', 'make', 'model', 'serial-number', 'comment',
                'exposure_time', 'current', 'voltage', 'iso', 'aperture','camera', 'filename']
    
    EL_Metadata = EL_Metadata[el_order]
    #EL_Metadata.insert(0, column='ID', value='')

    return EL_Metadata


def el_database_updater():
    # Load in old el database to get the last date
    old_el = dm.read_database(database_file_path).sort_values('date')
    last_date = old_el['date'].iloc[-1]

# Create list of folders to run stripper function on
    folder_list = fm.search_folders(last_date, NEW_DATA)

# Create new dataframe with new files
    new_el = parse_el_metadata(folder_list)

# Concat the two together, drop duplicates in case any were made
    updated_el_database = pd.concat(
        [old_el, new_el]).reset_index().drop(columns='index')
    final_el = updated_el_database.drop_duplicates(subset=['filename'])

# Save the dataframe
    dm.save_database(final_el, database_file_path)

    print('el_updated')
    return new_el


if __name__ == "__main__":
    el_database_updater()