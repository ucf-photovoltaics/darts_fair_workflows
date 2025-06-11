"""
@author: Sam Borges

Instrument Data Parser: Software for processing and parsing instrument data.

This script processes data from the following instruments:
    - Electroluminescence (EL) Images from DSLR/CMOS cameras
    - Sinton FMT (Flash Module Tester) measurements
    - IV (Current-Voltage) curve data
    - Module metadata and test conditions

The parser extracts and processes:
    - Image metadata (EXIF data, timestamps, camera settings)
    - Electrical measurements (IV curves, power measurements)
    - Test conditions (cycles, pressure, temperature)
    - Module identification and serial numbers

Events and Errors are tracked and logged in the console output.
"""

import os
import pandas as pd
import exifread
from PIL import Image
from PIL.TiffTags import TAGS
import idp.file_management as fm
import idp.SintonFMT_LIB as SintonFMT_LIB
import numpy as np

# Set pandas to display all columns but handle binary data
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 100)  # Limit column width to prevent binary data display

class InstrumentDataParser:
    def __init__(self, folder_locations, sqlite_file_path):
        self.folder_locations = folder_locations
        self.sqlite_file_path = sqlite_file_path
        self.failed_files = {}
        self.arrays = [
            'isc_array_raw', 'isc_array_interp', 'intensity_array', 
            'voc_array_raw', 'voc_array_interp', 'vload_array', 'vload_array_interp'
            ]

    def _format_array_data(self, data):
        """Format array data for display"""
        if isinstance(data, np.ndarray):
            return data
        elif isinstance(data, bytes):
            return np.frombuffer(data, dtype=np.float64)
        return data

    def extract_image_metadata(self, image_file):
        if image_file.endswith(".jpg"):
            with open(image_file, 'rb') as f:
                exif_data = exifread.process_file(f, details=False)
            # Convert bytes to string if necessary
            return {tag: str(value) if isinstance(value, bytes) else value.printable for tag, value in exif_data.items()}
        elif image_file.endswith(".tif"):
            with Image.open(image_file) as img:
                tiff_dict = {TAGS[key]: str(img.tag[key]) for key in img.tag.iterkeys()}
            return tiff_dict

    def get_files_from_folders(self, folder_list=None, filetype='txt', filename_only=True):
        if folder_list is None:
            folder_list = self.folder_locations
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

    def parse_image_metadata(self):
        failed_files = []
        el_filepaths = self.get_files_from_folders(filetype='jpg', filename_only=False)
        print(f'{len(el_filepaths)} new files found for EL')
        processed_data = []
        
        for index, file in enumerate(el_filepaths):
            try:
                metadata_dict = fm.get_filename_metadata(file, 'el')
                exif_data = self.extract_image_metadata(file)
                exif_data.update(metadata_dict)
                exif_data['filename'] = file.split('\\')[-1]
                exif_data['camera'] = 'EL_CCD'
                processed_data.append(exif_data)
            except Exception as e:
                print(f'{file} failed to be processed, please review. {e}')
                failed_files.append(file)
                continue
            if (index % 10 == 0):
                print(f'{index} EL Images processed so far...')
        
        print(f'Successfully processed {len(processed_data)} EL images')
        print(f'Failed to process {len(failed_files)} EL images')
        return processed_data, failed_files

    def parse_sinton_fmt_metadata(self):
        failed_files = []
        mfr_filepaths = self.get_files_from_folders(filetype='mfr', filename_only=False)
        txt_filenames = self.get_files_from_folders(filetype='txt', filename_only=True)
        print(f'{len(mfr_filepaths)} new files found ending in .MFR')
        print(f'{len(txt_filenames)} new files found ending in .TXT')
        
        processed_data = []
        for index, file in enumerate(mfr_filepaths):
            try:
                data, content = SintonFMT_LIB.import_raw_data_from_file(file)
                metadata_dict = fm.get_filename_metadata(file, datatype='iv')
                corrected_data = SintonFMT_LIB.correct_raw_data(data)
                interpol_data = SintonFMT_LIB.interpolate_load_data(corrected_data)
                
                # Convert array data to numpy arrays
                for array in self.arrays:
                    if array in interpol_data:
                        interpol_data[array] = np.array(interpol_data[array])
                
                content = {item.split('=')[0]: item.split('=')[1].replace('"', '').strip() for item in content if '=' in item}
                content.update(metadata_dict)
                content.update(interpol_data)
                content['filepath'] = file
                content['filename'] = file.split('\\')[-1]
                if content['filename'].replace('IVT', '').strip('.mfr') in txt_filenames:
                    content['txt_file'] = content['filename'].replace('mfr', 'txt')
                else:
                    content['txt_file'] = 'N/A'
                
                # Clean empty strings
                for key, value in content.items():
                    if isinstance(value, str) and value.strip() == "":
                        content[key] = None
                
                processed_data.append(content)
            except Exception as e:
                print(f'{file} failed to be processed, please review. {e}')
                failed_files.append(file)
                continue
            if (index % 10 == 0):
                print(f'{index} IV curves processed so far...')
        
        print(f'Successfully processed {len(processed_data)} IV curves')
        print(f'Failed to process {len(failed_files)} IV curves')
        return processed_data, failed_files

    def log_parsing_results(self, image_metadata, failed_image_files, sinton_metadata, failed_sinton_files):
        """Log the results of parsing image and Sinton metadata."""
        if image_metadata:
            print(f"\nImage metadata shape: {len(image_metadata)}")
        else:
            print("\nNo image metadata found")

        if sinton_metadata:
            print(f"Sinton metadata shape: {len(sinton_metadata)}")
        else:
            print("No Sinton metadata found")

        if failed_image_files:
            print("\nFailed to process the following image files:")
            for file in failed_image_files:
                print(f"- {file}")

        if failed_sinton_files:
            print("\nFailed to process the following Sinton files:")
            for file in failed_sinton_files:
                print(f"- {file}")

# Example usage:
# parser = InstrumentDataParser(folder_locations=['path/to/folders'], sqlite_file_path='path/to/sqlite.db')
# image_metadata, failed_image_files = parser.parse_image_metadata()
# sinton_metadata, failed_sinton_files = parser.parse_sinton_fmt_metadata() 