# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:18:34 2024

@author: Brent Thompson

This Metadata parsing script is designed to take raw data from the module
scanner and process it to new locations and extract cells images. Key functions
include parsing new data to construct a filename, using that filename to move
file, and extracting cell images. 

"""
import os
from datetime import datetime
import database_manipulation as dm
import file_management as fm
import pandas as pd
import xarray as xr
import cv2

# Path to UCF Onedrive storage, all processed files go here
# UCF_RT = 'C:/UCF/FSEC/Data/'
UCF_RT = 'C:/Users/NickEL/University of Central Florida/UCF_Photovoltaics_GRP - '
SCANNER_ONEDRIVE = UCF_RT + 'Module_Scanner/'

# Existing module scanner databases, one for NC files and one for JPG files
NC_DATABASE = UCF_RT + 'FSEC_PVMCF/module_databases/scanner-nc-metadata.txt'
JPG_DATABASE = UCF_RT + 'FSEC_PVMCF/module_databases/scanner-jpg-metadata.txt'
MODULES = UCF_RT + 'FSEC_PVMCF/module_databases/module-metadata.txt'

# Paths to final locations for different data
NC_FILES = SCANNER_ONEDRIVE + 'new_nc_files/'
JPG_FILES = SCANNER_ONEDRIVE + 'combined_files/'
EXTRACTED_EL = SCANNER_ONEDRIVE + 'extracted_EL/'
EXTRACTED_PL = SCANNER_ONEDRIVE + 'extracted_PL/'
EXTRACTED_UVF = SCANNER_ONEDRIVE + 'extracted_UVF/'

# Folder that NickEL outputs to, will contain any new data
# NEW_DATA = 'C:/UCF/FSEC/Data/New_Data/module_scanner/'
NEW_DATA = 'C:/Users/NickEL/TauScience/NickEL/data'

nc_metadata = ['date', 'time', 'module-id', 'frames',
               'comment', 'exposure-time', 'current', 'voltage', 'filename']

jpg_metadata = ['date', 'time', 'module-id', 'frames', 'comment',
                'exposure-time', 'current', 'voltage', 'filename', 'image-type']

module_metadata = ["module-id", "make", "model", "serial-number"]

columns_order = ["date", "time", "module-id", "make", "model", "serial-number",
                 "comment", "frames", "exposure-time", "current", "voltage", "filename"]


def get_new_scanner_files(folder_list):
    """
    Get any scanner files that are in the folder input with timestamp

    Uses the search_folders function from file_management to look at
    data folder and select new folders based off last date in table.

    Input is a string for foldername
    Returns filepaths with their time stamp of creation
    """
    nc_filepaths = []
    jpg_filepaths = []
    for folder in folder_list:
        for dirpath, dirname, filenames in os.walk(NEW_DATA):
            for filename in filenames:
                try:
                    file = os.path.join(dirpath, filename)
                    creation_time = os.path.getmtime(file)
                    time_stamp = datetime.fromtimestamp(
                        creation_time).strftime('%Y%m%d %H%M%S')
                    file_data = (file, time_stamp)

                    if filename.endswith(".nc"):
                        nc_filepaths.append(file_data)
                    elif filename.endswith(".jpg") and 'Cell' not in filename:
                        jpg_filepaths.append(file_data)
                except FileNotFoundError:
                    pass
    new_scanner_files = nc_filepaths, jpg_filepaths
    return new_scanner_files


def get_processed_nc_files():
    nc_files = []
    stripped_folder = []
    for dirpath, dirname, filenames in os.walk(NC_FILES):
        for filename in filenames:
            if filename.endswith('.nc'):
                nc_files.append(filename)

    for filename in nc_files:
        try:
            new_row = fm.get_filename_metadata(filename, 'scanner')

            date = new_row.get('date')
            time = new_row.get('time')
            module_id = new_row.get('module_id')
            serial_number = new_row.get('serial_number')
            make = new_row.get('make')
            model = new_row.get('model')
            frames = "5x"  # new_row.get('frames')
            comment = new_row.get('comment')
            exposure_time = new_row.get('exposure_time')
            current = new_row.get('current')
            voltage = new_row.get('voltage')

            metadata = [date, time, module_id, make, model, serial_number,
                        comment, frames, exposure_time, current, voltage, filename]

            stripped_folder.append(metadata)
        except IndexError:
            print(filename)

    all_nc_files = pd.DataFrame(stripped_folder, columns=columns_order)
    return all_nc_files


def get_processed_jpg_files():
    jpg_files = []
    stripped_folder = []
    for dirpath, dirname, filenames in os.walk(JPG_FILES):
        for filename in filenames:
            if filename.endswith('.jpg'):
                jpg_files.append(filename)

    for filename in jpg_files:
        try:
            new_row = fm.get_filename_metadata(filename, 'scanner')

            date = new_row.get('date')
            time = new_row.get('time')
            module_id = new_row.get('module_id')
            serial_number = new_row.get('serial_number')
            make = new_row.get('make')
            model = new_row.get('model')
            frames = "5x"  # new_row.get('frames') FIXME need to get old frames
            comment = new_row.get('comment')
            exposure_time = new_row.get('exposure_time')
            current = new_row.get('current')
            voltage = new_row.get('voltage')
            image_type = new_row.get('image_type')

            metadata = [date, time, module_id, make, model, serial_number,
                        comment, frames, exposure_time, current, voltage, filename, image_type]

            stripped_folder.append(metadata)
        except IndexError:
            print(filename)

    jpg_columns_order = columns_order[:]
    jpg_columns_order.append("image-type")
    all_jpg_files = pd.DataFrame(stripped_folder, columns=jpg_columns_order)
    return all_jpg_files


def nc_database_updater(nc_files):
    """
    NC files are automatically produced by the scanner and follow the format
    <moduleid_framestaken_exposuretime> ie FPCAL-0001_5x_25ms.nc
    with additional metadata such as comment, voltage, current extending base
    <moduleid_framestaken_exposuretime_comment_voltage_current>

    Once metadata is stripped, a dataframe is returned

    F2408-0004_5x_IR90ms-EL jpg output format
    """

    stripped_folder = []

    for file in nc_files:
        try:
            file_name = file[0].split('\\')[-1].split('.')[0].split('_')
            file_path = file[0].replace('\\', '/')
            if len(file_name) < 3:
                continue
            date = file[1][:8]
            time = file[1][9:]
            module_id = file_name[0]
            frames_taken = file_name[1]
            exposure_time = file_name[2].strip('IR')

            metadata = [date, time, module_id, frames_taken, exposure_time]

            if len(file_name) == 4:
                comment = file_name[3]
                voltage = 'V'
                current = 'A'
            elif len(file_name) == 5:
                comment = file_name[3]
                voltage = file_name[4]
                current = file_name[5]
            else:
                comment, voltage, current = 'X', 'A', 'V'
            metadata.extend([comment, voltage, current, file_path])
            metadata[4], metadata[5] = metadata[5], metadata[4]
        except IndexError:
            print('Index Error')
            metadata = ['!', '!', '!', '!', '!', '!', '!', '!', file_path]

        stripped_folder.append(metadata)

    nc_dataframe = pd.DataFrame(stripped_folder, columns=nc_metadata)
    modules = pd.read_csv(
        MODULES, sep='\t', usecols=module_metadata)
    nc_dataframe = nc_dataframe.merge(
        modules, how='left', left_on="module-id", right_on="module-id")

    nc_dataframe = nc_dataframe[columns_order]
    return nc_dataframe


def rename_nc_files(nc_dataframe):
    """
    date_time_make_model_sn_comment_Xms_A_V.nc
    """
    new_nc_files = []
    values_for_filename = [0, 1, 3, 4, 5, 6, 8, 9, 10]
    for index, row in nc_dataframe.iterrows():
        try:
            file_name = row.iloc[values_for_filename].str.cat(sep='_') + ".nc"
            new_name = NC_FILES + row.iloc[0] + '/' + file_name
            new_nc_files.append(new_name)
            old_name = row.iloc[[-1]][0]
            os.renames(old_name, new_name)

            nc_dataframe.at[index, 'filename'] = file_name
        except FileExistsError:
            pass
        except FileNotFoundError:
            pass
    return new_nc_files, nc_dataframe


def jpg_database_updater(jpg_files):
    """

    F2408-0004_5x_IR90ms-EL jpg output format
    """

    stripped_folder = []
    image_types = ['EL', 'PL', 'UVF']
    for file in jpg_files:
        try:
            file_name = file[0].split('\\')[-1].split('.')[0].split('_')
            file_path = file[0].replace('\\', '/')
            if len(file_name) < 3:
                continue
            date = file[1][:8]
            time = file[1][9:]
            module_id = file_name[0]
            frames_taken = file_name[1]

            for i in image_types:
                if i in file_name[2]:
                    image_type = i
            exposure_time = file_name[2].strip('IR').strip(f'-{image_type}')

            metadata = [date, time, module_id, frames_taken, exposure_time]

            if len(file_name) == 4:
                comment = file_name[3].strip(f'-{image_type}')
                voltage = 'V'
                current = 'A'
            elif len(file_name) == 5:
                comment = file_name[3].strip(f'-{image_type}')
                voltage = file_name[4]
                current = file_name[5]
            else:
                comment, voltage, current = 'X', 'A', 'V'
            metadata.extend([comment, voltage, current, file_path, image_type])
            metadata[4], metadata[5] = metadata[5], metadata[4]
        except IndexError:
            print('Index Error')
            metadata = ['!', '!', '!', '!', '!', '!', '!', '!', file_path]

        stripped_folder.append(metadata)

    jpg_dataframe = pd.DataFrame(stripped_folder, columns=jpg_metadata)
    modules = pd.read_csv(
        MODULES, sep='\t', usecols=module_metadata)
    jpg_dataframe = jpg_dataframe.merge(
        modules, how='left', left_on="module-id", right_on="module-id")

    jpg_columns_order = columns_order[:]
    jpg_columns_order.append("image-type")

    jpg_dataframe = jpg_dataframe[jpg_columns_order]
    return jpg_dataframe


def rename_jpg_files(jpg_dataframe):
    """
    date_time_make_model_sn_comment_Xms_A_V_imagetype.jpg

    """

    values_for_filename = [0, 1, 3, 4, 5, 6, 8, 9, 10, 12]
    for index, row in jpg_dataframe[:].iterrows():

        try:
            file_name = row.iloc[values_for_filename].str.cat(sep='_') + ".jpg"
            new_name = JPG_FILES + row.iloc[0] + '/' + file_name
            old_name = row.iloc[[-2]][0]
            os.renames(old_name, new_name)

            jpg_dataframe.at[index, 'filename'] = file_name
        except FileExistsError:
            pass
        except FileNotFoundError:
            pass
    return jpg_dataframe


def read_nc_file(nc_file, image_type='EL'):
    '''
    Options for image_type: EL, PL, UVF
    '''
    data_structure = xr.load_dataset(nc_file)
    image_metadata = data_structure.attrs

    cell_images = [data_structure.InGaAs[i].loc['EL']
                   for i in range(len(data_structure.InGaAs))]
    cell_images = [cell_image.values for cell_image in cell_images]

    return cell_images, image_metadata


def save_cells_from_nc(new_nc_files):
    for nc_file in new_nc_files:
        data_structure = xr.load_dataset(nc_file)
        image_metadata = data_structure.attrs
        date = nc_file.split('/')[-1].split('_')[0]
        cell_filename = nc_file.split('/')[-1].strip('.nc') + '_cell_'

        if 'EL' in image_metadata['Motion']:
            EL_cell_images = [data_structure.InGaAs[i].loc['EL']
                              for i in range(len(data_structure.InGaAs))]
            EL_cell_images = [cell.values for cell in EL_cell_images]
            EL_cell_root = EXTRACTED_EL + date + '/'
            if not os.path.isdir(EL_cell_root):
                os.mkdir(EL_cell_root)

            for index, cell in enumerate(EL_cell_images):
                cell_number = (f'{(index + 1):03}')
                image_path = f'{EL_cell_root}/{cell_filename}{cell_number}.jpg'
                cv2.imwrite(image_path, cell)

        if 'PL' in image_metadata['Motion']:
            PL_cell_images = [data_structure.InGaAs[i].loc['PL']
                              for i in range(len(data_structure.InGaAs))]
            PL_cell_images = [cell.values for cell in PL_cell_images]
            PL_cell_root = EXTRACTED_PL + date + '/'
            if not os.path.isdir(PL_cell_root):
                os.mkdir(PL_cell_root)

            for index, cell in enumerate(PL_cell_images):
                cell_number = (f'{(index + 1):03}')
                image_path = f'{PL_cell_root}/{cell_filename}{cell_number}.jpg'
                cv2.imwrite(image_path, cell)

    return new_nc_files


def run():
    # Load old dataset
    old_nc_dataset = dm.read_database(NC_DATABASE)
    old_jpg_dataset = dm.read_database(JPG_DATABASE)

    # Get last date from old dataset
    nc_last_date = old_nc_dataset['date'].iloc[-1]

    # Search for new folders using last date
    nc_folder_list = fm.search_folders(nc_last_date, NEW_DATA)

    # Gather files from new folder
    nc_files, jpg_files = get_new_scanner_files(nc_folder_list)

    # Create dataframe from new files
    new_nc_dataset = nc_database_updater(nc_files)
    new_jpg_dataset = jpg_database_updater(jpg_files)

    # Rename and Move the files to new locations
    new_nc_files, new_nc_dataset = rename_nc_files(new_nc_dataset)
    new_jpg_dataset = rename_jpg_files(new_jpg_dataset)

    # Extract cell images from NC files
    save_cells_from_nc(new_nc_files)

    # Join new datasets on old, save as txt file
    final_nc_dataset = pd.concat(
        [old_nc_dataset, new_nc_dataset]).reset_index().drop(columns='index')
    final_nc_dataset = final_nc_dataset.drop_duplicates(subset=['filename'])

    final_jpg_dataset = pd.concat(
        [old_jpg_dataset, new_jpg_dataset]).reset_index().drop(columns='index')
    final_jpg_dataset = final_jpg_dataset.drop_duplicates(subset=['filename'])

    # Save files in module database
    dm.save_database(final_nc_dataset, NC_DATABASE)
    dm.save_database(final_jpg_dataset, JPG_DATABASE)

    return new_nc_dataset, new_jpg_dataset


if __name__ == "__main__":
    run()

    """
dm.save_database(all_nc_files, NC_DATABASE)    
def process_old_dataset_format():
    old_nc_dataset = dm.read_database(NC_DATABASE)
    old_jpg_dataset = dm.read_database(JPG_DATABASE)
    old_nc_dataset = old_nc_dataset.drop(
        columns={'source_filepath', 'destination_filename'})
    old_jpg_dataset = old_jpg_dataset.drop(
        columns={'source_filepath', 'destination_filename'})
    dm.save_database(old_nc_dataset, NC_DATABASE)
    dm.save_database(old_jpg_dataset, JPG_DATABASE)

    
    
    
    
    # Uses the current output from NickEl to contruct data entry
    for file in nc_filepaths:
        file_name = file[0].split('\\')[-1].split('.')[0].split('_')
        nc_dict['source_filepath'] = file[0]
        nc_dict['date'] = file[1][:8]
        nc_dict['time'] = file[1][9:]
        nc_dict['module-id'] = file_name[0]
        nc_dict['frames_taken'] = file_name[1]
        nc_dict['exposure_time'] = file_name[2]
        try:
            nc_dict['comment'] = file_name[3]
            if file_name[4]:
                nc_dict['voltage'] = file_name[4]
                nc_dict['current'] = file_name[5]
        except ValueError:
            nc_dict['comment, voltage, current'] = 0
        #new_row = pd.DataFrame(nc_dict, Scanner_metadata)

        #old_nc = old_nc.append(nc_dict, ignore_index=True)
"""
#
