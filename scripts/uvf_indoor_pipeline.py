# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 14:39:12 2024

@author: PixEL
"""

import os
import exifread

import pandas as pd
import idp.file_management as fm
import database_manipulation as dm


#NEW_DATA = 'C:/UCF/FSEC/Data/New_Data/Dark_IV/'
NEW_DATA = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/Instrument_Data/UVF_Images'
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'

# database_file_path = 'C:/UCF/FSEC/Data/Databases/
database_file_path = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/uvf-indoor-metadata.txt'

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


def parse_indoor_uvf_metadata(folder_list):
    indoor_uvf_metadata = [
    'date', 'time', 'make', 'model', 'serial-number', 'comment',
    'exposure_time', 'iso', 'aperature', 'camera', 'flash_source', 'filename']
    indoor_uvf_filepaths = []
    stripped_folder = []
    failed_files = []

    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(".jpg"):
                    file = os.path.join(dirpath, filename)
                    indoor_uvf_filepaths.append(file)

    print(f'{len(indoor_uvf_filepaths)} new files found for Indoor UVF')

    for file in indoor_uvf_filepaths:
        try:
            metadata_dict = fm.get_filename_metadata(file, 'uvf')
            exif_data = extract_EXIF_data(file)

            filename = file.split('\\')[-1]

            date = metadata_dict.get('date')
            time = metadata_dict.get('time')
            make = metadata_dict.get('make')
            model = metadata_dict.get('model')
            serial = metadata_dict.get('serial_number')
            comment = metadata_dict.get('comment')
            
            iso = exif_data.get('EXIF ISOSpeedRatings').printable
            aperature = exif_data.get('EXIF FNumber').printable
            exposure_time = exif_data.get('EXIF ExposureTime').printable
            flash_source = "YONGNUO-YN660"
            camera = "NIKON-D3500"
            
            metadata = [date, time, make, model, serial, comment, exposure_time,
                        iso, aperature, camera, flash_source, filename]
            
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


            

    Indoor_UVF_Metadata = pd.DataFrame(stripped_folder, columns=indoor_uvf_metadata)

    Indoor_UVF_Metadata = dm.join_module_metadata(Indoor_UVF_Metadata)
    
    uvf_order = [
    'date', 'time', 'module-id', 'make', 'model', 'serial-number', 'comment',
    'exposure_time', 'filename']
    
    Indoor_UVF_Metadata = Indoor_UVF_Metadata[uvf_order]
    #Indoor_UVF_Metadata.insert(0, column='ID', value='')

    return Indoor_UVF_Metadata


def indoor_uvf_database_updater():
    # Load in old dark_iv database to get the last date
    old_uvf = dm.read_database(database_file_path).sort_values('date')
    last_date = old_uvf['date'].iloc[-1]

# Create list of folders to run stripper function on
    folder_list = fm.search_folders(last_date, NEW_DATA)

# Create new dataframe with new files
    new_uvf = parse_indoor_uvf_metadata(folder_list)

# Concat the two together, drop duplicates in case any were made
    updated_uvf_database = pd.concat(
        [old_uvf, new_uvf]).reset_index().drop(columns='index')
    final_uvf = updated_uvf_database.drop_duplicates(subset=['filename'])

# Save the dataframe
    dm.save_database(final_uvf, database_file_path)

    print('indoor_uvf_updated')
    return new_uvf


if __name__ == "__main__":
    indoor_uvf_database_updater()