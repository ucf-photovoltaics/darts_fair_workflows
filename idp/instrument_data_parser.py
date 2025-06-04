# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 13:54:37 2024

@author: Brent Thompson

This script is meant to replace the series of scripts that are in
the database folder
"""
import os
import pandas as pd
import idp.file_management as fm
import exifread
import idp.SintonFMT_LIB as SintonFMT_LIB
from PIL import Image
from PIL.TiffTags import TAGS

arrays = ['isc_array_raw', 'isc_array_interp',
          'intensity_array', 'voc_array_raw', 'voc_array_interp',
          'vload_array', 'vload_array_interp']


def hextostr(v):
    if v.startswith(b'0x'):
        v = v[2:]
    b = bytes.fromhex(v.lower())
    return b.decode('utf-8')

def serialize_array(arr):
    return arr.tobytes()


def extract_image_metadata(image_file):
    
    if image_file.endswith(".jpg"):
        with open(image_file, 'rb') as f:
            exif_data = exifread.process_file(f, details=False)
        return {tag: value.printable for tag, value in exif_data.items()}
    elif image_file.endswith(".tif"):
        with Image.open(image_file) as img:
            tiff_dict = {TAGS[key] : img.tag[key] for key in img.tag.iterkeys()}
        return tiff_dict

# Algorithm to go through entire folder, and select each file for processing

def get_files_from_folders(folder_list=[], filetype='txt', filename_only=True):
    """
    Used to traverse the filesystem looking for a specific file type, folder
    list is an output of the Search Folder function
    """
    filetype_folder = []
    for folder in folder_list:
        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                if filename.endswith(f".{filetype}"):
                    if filename_only:
                        filetype_folder.append(filename.split('.')[0])
                    else:
                        file = os.path.join(dirpath, filename)
                        filetype_folder.append(file)
    return filetype_folder


def list_to_dict(lst):
    """
    Used to take the output of sintonLib function and create dict
    """
    result = {}
    for item in lst:
        try:
            key, value = item.split('=')
            result[key] = value.replace('"', '').strip()
        except Exception:
            pass
    return result


def parse_sinton_fmt_metadata(folder_list):
    """
    Parameters
    ----------
    folder_list : List of lists, each list being a date folder that contains the MFRs
        DESCRIPTION.

    Returns
    -------
    Pandas dataframe representing the tabular form of the data passed in as folders. 

    """

# Lines from the MFR file output from the sinton

    failed_files = []

    mfr_filepaths = get_files_from_folders(
        folder_list, filetype='mfr', filename_only=False)
    txt_filenames = get_files_from_folders(
        folder_list, filetype='txt', filename_only=True)

    print(f'{len(mfr_filepaths)} new files found ending in .MFR')
    print(f'{len(txt_filenames)} new files found ending in .TXT')

    pd.set_option('display.max_colwidth', None)

# Algorithm to go through each mrf file, and extract relevent information
    for index, file in enumerate(mfr_filepaths):

        # Parse files for metadata and data contained within text files
        try:
            # Use sinton library to extract, correct, and interpolate iv data
            data, content = SintonFMT_LIB.import_raw_data_from_file(file)
            metadata_dict = fm.get_filename_metadata(file, datatype='iv')

            corrected_data = SintonFMT_LIB.correct_raw_data(data)
            interpol_data = SintonFMT_LIB.interpolate_load_data(corrected_data)

            for array in arrays:
                interpol_data[array] = interpol_data[array].tobytes()

            #interpol_data[array] = " ".join(str(x) for x in interpol_data[array])

            # Turn contents of MFR file to dictonary
            content = list_to_dict(content)

            # Update with module metadata and extracted iv data
            content.update(metadata_dict)
            content.update(interpol_data)

            content['filepath'] = file
            content['filename'] = file.split('\\')[-1]  # Add

            if content['filename'].replace('IVT', '').strip('.mfr') in txt_filenames:
                content['txt_file'] = content['filename'].replace('mfr', 'txt')
            else:
                content['txt_file'] = 'N/A'

            if index == 0:
                frames = pd.DataFrame.from_dict(
                    content, orient='index').T
            else:
                frames = pd.concat(
                    [frames, pd.DataFrame([content])], ignore_index=True)

                # frames = frames.append(frame) ## Doesnt work in 3.11

        except ValueError:
            failed_files.append(file)
            print(f'{file} failed to be processed, please review')
            continue
        except IndexError:
            failed_files.append(file)
            print(f'{file} failed to be processed, please review')
            continue

        if (index % 10 == 0):
            print(f'{index} IV curves processed so far...')

        # Dropping N/A and blank values
    nan_value = float("NaN")
    frames.replace("", nan_value, inplace=True)
    frames = frames.loc[:, frames.notna().all(axis=0)]

    Sinton_IV_Metadata = frames.reset_index()
    #update_database.blank_insert_to_database('mfr', frames)

    return Sinton_IV_Metadata, failed_files


def parse_image_metadata(folder_list):
    failed_files = []

    el_filepaths = get_files_from_folders(
        folder_list, filetype='jpg', filename_only=False)

    print(f'{len(el_filepaths)} new files found for EL')

    for index, file in enumerate(el_filepaths):
        try:
            metadata_dict = fm.get_filename_metadata(file, 'el')
                
            exif_data = extract_image_metadata(file)

            exif_data.update(metadata_dict)

            exif_data['filename'] = file.split('\\')[-1]

            frame = pd.DataFrame.from_dict(exif_data, orient='index').T

            if index == 0:
                frames = frame 
            else:
                frames = pd.concat(
                    [frames, frame], ignore_index=True)
            
        except ValueError as ve:
            print(f'{file} failed to be processed, please review. {ve}')
            failed_files.append(file)
            continue
        except AttributeError as ae:
            print(f'{file} failed to be processed, please review. {ae} ')
            failed_files.append(file)
            continue
        except IndexError:
            failed_files.append(file)
            print(f'{file} failed to be processed, please review')
            continue
        if (index % 10 == 0):
            print(f'{index} EL Images processed so far...')

    frames = frames.dropna(axis=1, how='all')
    frames = frames.loc[:, frames.notna().all(axis=0)]
    
    w['camera'] = 'EL_CCD'

    Image_Metadata = frames.reset_index()

    return Image_Metadata, failed_files


"""
folder_list = fm.search_folders()

el2['serial_number'] = el2['serial_number'].replace(
    to_replace='110505210092024',value='11505210092024')

el2.loc[el2['serial_number'].str.len() == 4, 'serial_number'] = el2['serial_number'].replace(
    to_replace='2024', value='11505210092024')

el2, ff = parse_image_metadata(folder_list)


dm.blank_insert_to_database('elsave', el2)


last_date = update_database.get_last_date('sinton-iv-metadata')
folder_list = fm.search_folders(last_date)


pd.set_option('display.max_colwidth', None)

iv2, failed_files2 = parse_sinton_fmt_metadata(folder_list)
iv = iv.astype(str)


update_database.blank_insert_to_database('iv', iv)
"""