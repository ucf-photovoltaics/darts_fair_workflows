# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 17:08:49 2021

@author: PixEL
"""

import tkinter as tk
from tkinter import filedialog
import os.path
from os.path import basename
import pandas as pd
import os
import shutil
root = tk.Tk()
root.withdraw()


# ============General functions for file manipulation========= #
def get_files(title='Select files.'):
    '''
    Prompt user to select file or files.

    Parameters
    ----------
    title : str, optional
        Title on the dialog box. This is useful if this is called several
        times and the user needs to distinguish which type of files to open.
        The default is 'Select files.'.

    Returns
    -------
    files : tuple
        Full file paths of selected files.

    '''
    root = tk.Tk()
    files = list(filedialog.askopenfilenames(title=title))
    root.destroy()
    return files


def get_dir(title='Select directory.'):
    '''
    Prompt user to select a directory.

    Parameters
    ----------
    title : str, optional
        Title on the dialog box. This is useful if this is called several
        times and the user needs to distinguish which directory to open.
        The default is 'Select directory.'.

    Returns
    -------
    found_dir : str
        Directory path.

    '''
    root = tk.Tk()
    found_dir = filedialog.askdirectory(title=title)
    found_dir = f"{found_dir}/"
    root.destroy()
    return found_dir


def copy_files(files_to_copy, dst=None):
    '''
    Copies files to specified directory.

    Parameters
    ----------
    files_to_copy : tuple
        Tuple of files to copy to dst.
    dst : str
        Destination directory to copy the files into. Default is None. A prompt
        will appear is no dst is specified.

    Returns
    -------
    None.

    '''
    # Prompt user if dst is not defined
    if not os.path.isdir(dst):
        dst = filedialog.askdirectory(
            title='Select directory to store copied files.')

    newnames = []
    for n, file in enumerate(files_to_copy):
        dst_file = f"{dst}/{basename(file)}"
        # prevents writing errors when // accidentally used
        dst_file = dst_file.replace('//', '/')
        if not os.path.isfile(dst_file):
            try:
                shutil.copyfile(file, dst_file)
                newnames.append(dst_file)
                print(f"Copied {n+1} of {len(files_to_copy)}.")
            except:
                pass

    return newnames


def rename_file(full_filename, new_filename):
    os.rename(full_filename, new_filename)
    return print(new_filename)


def get_filename_metadata(file, datatype='iv'):
    '''
    Extracts the metadata from the filename string based on FSEC PVMCF filename
    standards for each measurement type.

    Parameters
    ----------
    file : str
        File path string.
    datatype : str
        Datetype defines which type of measurement you are reading data from.
        Datatype choices: 'iv' 'el' 'dark_iv' 'ir' 'uvf'. The default is 'iv'.

    Returns
    -------
    metadata_dict : dict
        Dictionary of metadata obtained from splitting the filename string.

    '''
    metadata_dict = {}
    bn_split = basename(file).split('_')
    ext = file.split('.')[-1]

    if datatype == 'iv':
        if ext == 'txt':
            metadata_dict['date'] = bn_split[0].split('-')[0]
            metadata_dict['time'] = bn_split[2]
            metadata_dict['make'] = bn_split[0].split('-')[1]
            metadata_dict['model'] = bn_split[1].replace(
                f"-{metadata_dict['date']}", '')
            metadata_dict['serial_number'] = bn_split[3]
            metadata_dict['comment'] = bn_split[4]
            metadata_dict['measurement_number'] = bn_split[5].replace(
                f".{ext}", '')
        else:
            metadata_dict['date'] = bn_split[0].replace(
                'IVT', '').split('-')[0]
            metadata_dict['time'] = bn_split[2]
            metadata_dict['make'] = bn_split[0].split('-')[1]
            metadata_dict['model'] = bn_split[1].replace(
                f"-{metadata_dict['date']}", '')
            metadata_dict['serial_number'] = bn_split[3]
            metadata_dict['comment'] = bn_split[4]
            metadata_dict['measurement_number'] = bn_split[5].replace(
                f".{ext}", '')
    elif datatype == 'el':
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['make'] = bn_split[2]
        metadata_dict['model'] = bn_split[3]
        metadata_dict['serial_number'] = bn_split[4]
        metadata_dict['comment'] = bn_split[5]
        metadata_dict['exposure_time'] = bn_split[6].replace('s', '')
        metadata_dict['current'] = bn_split[7].replace('A', '')
        metadata_dict['voltage'] = bn_split[8].replace(f"V.{ext}", '')
    elif datatype == 'ir':
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['make'] = bn_split[2]
        metadata_dict['model'] = bn_split[3]
        metadata_dict['serial_number'] = bn_split[4]
        metadata_dict['comment'] = bn_split[5]
        metadata_dict['exposure_time'] = bn_split[6].replace('s', '')
        metadata_dict['current'] = bn_split[7].replace(f"A.{ext}", '')
    elif datatype == 'dark_iv':
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['make'] = bn_split[2]
        metadata_dict['model'] = bn_split[3]
        metadata_dict['serial_number'] = bn_split[4]
        metadata_dict['comment'] = bn_split[5].replace(f".{ext}", '')
    elif datatype == 'uvf':
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['make'] = bn_split[2]
        metadata_dict['model'] = bn_split[3]
        metadata_dict['serial_number'] = bn_split[4]
        metadata_dict['comment'] = bn_split[5].replace(f".{ext}", '')
    elif datatype == 'v10':
        metadata_dict['serial-number'] = bn_split[4]
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['delay-time-(s)'] = bn_split[6].split('s')[0]
        metadata_dict['setpoint-total-time-(s)'] = bn_split[5].replace('s', '')
    elif datatype == 'scanner':
        metadata_dict['date'] = bn_split[0]
        metadata_dict['time'] = bn_split[1]
        metadata_dict['module_id'] = bn_split[2]
        metadata_dict['make'] = bn_split[3]
        metadata_dict['model'] = bn_split[4]
        metadata_dict['serial_number'] = bn_split[5]
        metadata_dict['exposure_time'] = bn_split[6]
        metadata_dict['current'] = bn_split[7]
        metadata_dict['voltage'] = bn_split[8]
        metadata_dict['comment'] = bn_split[9].split('.')[0]
        if ext == 'jpg':
            metadata_dict['image_type'] = bn_split[10].split('.')[0]
            if metadata_dict['image_type'] == 'cell':
                metadata_dict['cell_number'] = bn_split[11]

    return metadata_dict


def search_folders(date_threshold=20000000, parent_folder_path=''):
    """
    Uses a date threshold to select all folders in given parent path that beyond
    the given date. Returns a list of folders.
    """

    if not os.path.isdir(parent_folder_path):
        parent_folder_path = filedialog.askdirectory(
            title='Select source of data files to search through.')

    folders = []
    for dirpath, dirnames, filenames in os.walk(parent_folder_path):
        for dirname in dirnames:
            dirname = dirname.replace('-', '')
            try:
                if int(dirname) >= int(date_threshold):
                    new_folder = os.path.join(dirpath, dirname)
                    folders.append(new_folder)
                    print(f'{new_folder} added for processing.')
            except ValueError:
                print(f'{dirname} skipped.')
                pass
    return folders


def get_directory_names(source):
    """
    Uses os.walk to return a list of directories, ie date folders.
    """
    directory_names = []
    for dirpath, dirnames, filenames in os.walk(source):
        for name in dirnames:
            directory_names.append(name)
            print(name)
    return directory_names


def search_files(serial_numbers=None, instrument_data_path=''):

    if not serial_numbers:
        raise ValueError('Serial numbers list was empty.')

    # Prompt user if src is not defined
    if not os.path.isdir(instrument_data_path):
        instrument_data_path = filedialog.askdirectory(
            title='Select source of data files to search through.')

    src_dict = {'iv': f"{instrument_data_path}/Sinton_FMT/Results/MultiFlash/",
                'el': f"{instrument_data_path}/EL_DSLR_CMOS/",
                'darkiv': f"{instrument_data_path}/Dark_IV_Data/",
                'ir': f"{instrument_data_path}/IR_ICI/",
                'uvf': f"{instrument_data_path}/UVF_Images/",
                'spire': f"{instrument_data_path}/Spire/Data/",
                'v10': f"{instrument_data_path}/V10/"
                }

    # get all files in src in a list
    instrument_data_dict = {}
    # search through each measurement data type individually
    for measurement, measurement_data_src in src_dict.items():
        all_files = []
        print(f"Searching for {measurement} files.")

        for (dirpath, dirnames, filenames) in os.walk(measurement_data_src):
            [all_files.append(f"{dirpath}/{f}")
             for f in filenames if any(sn in f for sn in serial_numbers)]
            # Prevents errors when two forward slashes are used in a row
            all_files = [a.replace('//', '/') for a in all_files]
            instrument_data_dict[measurement] = all_files

    return instrument_data_dict


def retrieve_module_data(serial_number, instrument_data_path):
    src_dict = {'iv': f"{instrument_data_path}/Sinton_FMT/Results/MultiFlash/",
                'el': f"{instrument_data_path}/EL_DSLR_CMOS/",
                'darkiv': f"{instrument_data_path}/Dark_IV_Data/",
                'ir': f"{instrument_data_path}/IR_ICI/",
                'uvf': f"{instrument_data_path}/UVF_Images/",
                'spire': f"{instrument_data_path}/Spire/Data/",
                'v10': f"{instrument_data_path}/V10/"
                }
    instrument_data_dict = {}

    for measurement, measurement_data_src in src_dict.items():
        all_files = []
        print(f"Searching for {measurement} files.")

        for (dirpath, dirnames, filenames) in os.walk(measurement_data_src):
            [all_files.append(f"{dirpath}/{f}")
             for f in filenames if serial_number in f]
            all_files = [a.replace('//', '/') for a in all_files]
            instrument_data_dict[measurement] = all_files
    return serial_number, instrument_data_dict


def copy_data_to_folder(instrument_data_dict=None, dst=None, raw_el_images=True):

    # If there is no dst folder specified, prompt user
    if not dst:
        dst = filedialog.askdirectory(
            title='Select folder where files will be copied to.')

    if not raw_el_images:
        instrument_data_dict['el'] = [
            image_file for image_file in instrument_data_dict['el'] if image_file[-3:].upper() == 'JPG']

    for measurement in instrument_data_dict:
        dst_measurement_dir = f"{dst}/{measurement.upper()}/"
        dst_measurement_dir = dst_measurement_dir.replace('//', '/')
        if not os.path.isdir(dst_measurement_dir):
            os.mkdir(dst_measurement_dir)
        print(
            f"Begin copying {measurement} files: {len(instrument_data_dict[measurement])} found.")
        copy_files(instrument_data_dict[measurement], dst=dst_measurement_dir)
        print(
            f"Finished copying {len(instrument_data_dict[measurement])} {measurement.upper()} files.")


def search_and_copy_files(serial_numbers=None, instrument_data_path='', dst=None, raw_el_images=True):
    instrument_data_dict = search_files(serial_numbers, instrument_data_path)
    copy_data_to_folder(instrument_data_dict, dst, raw_el_images)


def get_files_in_directory(source_dir=None):
    if not source_dir:
        source_dir = get_dir(
            'Select source directory from which you want to load all files.')

    all_files = []
    for (dirpath, dirnames, filenames) in os.walk(source_dir):
        [all_files.append(f"{dirpath}/{f}") for f in filenames]
        # Prevents errors when two forward slashes are used in a row
        all_files = [a.replace('//', '/') for a in all_files]

    return all_files


# === THE FOLLOWING FUNCTIONS ARE USED IN THE COMPLETE ANALYSIS PIPELINE === #
#================= Sinton I-V related functions =================#


def get_latest_iv_files(iv_files):
    '''
    This allows the user to select all I-V files without
    manually selecting the third measurement each time.

    Parameters
    ----------
    iv_files : tuple or list
        All files to search through and filter.

    Returns
    -------
    iv_files_latest: list
        Filtered list of I-V files including the most recent
        I-V measurements.
    '''
    iv_files_latest = []

    times = []
    sns = []
    for f in iv_files:
        bn_split = basename(f).split('_')
        n = 4
        sn = bn_split[n]
        time = bn_split[n-1]
        times.append(time)
        sns.append(sn)

    iv_info = pd.DataFrame({"files": iv_files,
                            "sn": sns,
                            "times": times})

    for g in iv_info.groupby('sn'):
        iv_files_latest.append(
            list(g[1].sort_values('times').reset_index().files)[-1])

    return iv_files_latest


def label_module(filename_string, datatype='iv',
                 order=['date', 'serial_number']):
    metadata = get_filename_metadata(filename_string, datatype='iv')

    # Options for order are below
    date_m = metadata['date']
    time_m = metadata['time']
    make = metadata['make']
    model = metadata['model']
    sn = metadata['serial_number']
    sn_short = sn[-4:]

    comment = metadata['comment']

    options = {'date': date_m, 'time': time_m, 'make': make, 'model': model,
               'serial_number': sn, 'serial_number_short': sn_short,
               'comment': comment}

    label = "_".join([options[i] for i in order])
    return label, options
