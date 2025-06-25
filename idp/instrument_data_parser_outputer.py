import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
import time
import matplotlib as mpl
from scripts.sqlite_operations import SQLiteDB
from .instrument_data_parser_plotter import InstrumentDataParserPlotter

class InstrumentDataParserOutputer:
    def __init__(self, output_dir=None):
        # Initialize storage for DataFrames
        self.dataframes = {
            'el': None,
            'sinton': None
        }
        # Set default output directory if none provided
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
        self.output_dir = output_dir
        # Create timestamped base directory
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.timestamped_dir = os.path.join(self.output_dir, self.timestamp)
        os.makedirs(self.timestamped_dir, exist_ok=True)
        # Create dataset-specific directories with visualization type subdirectories
        self.dataset_dirs = {
            'el': {
                'numeric_histograms': os.path.join(self.timestamped_dir, 'el', 'numeric_histograms'),
                'correlations': os.path.join(self.timestamped_dir, 'el', 'correlations'),
                'categorical': os.path.join(self.timestamped_dir, 'el', 'categorical'),
                'time_series': os.path.join(self.timestamped_dir, 'el', 'time_series'),
                # 'csv': os.path.join(self.timestamped_dir, 'el', 'csv')
            },
            'sinton': {
                'numeric_histograms': os.path.join(self.timestamped_dir, 'sinton', 'numeric_histograms'),
                'correlations': os.path.join(self.timestamped_dir, 'sinton', 'correlations'),
                'scatter_matrix': os.path.join(self.timestamped_dir, 'sinton', 'scatter_matrix'),
                'categorical': os.path.join(self.timestamped_dir, 'sinton', 'categorical'),
                'time_series': os.path.join(self.timestamped_dir, 'sinton', 'time_series'),
                # 'csv': os.path.join(self.timestamped_dir, 'sinton', 'csv')
            }
        }
        # Create all directories
        for dataset in self.dataset_dirs.values():
            for dir_path in dataset.values():
                os.makedirs(dir_path, exist_ok=True)
        # Setup database path
        db_path = os.path.join(self.output_dir, 'database', 'instrument_data.db')
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db = SQLiteDB(database_path=db_path)
        # Initialize plotter
        self.plotter = InstrumentDataParserPlotter(self.output_dir, self.timestamp, self.dataset_dirs)

    def _sanitize_filename(self, filename):
        """Sanitize a string to be used as a filename by replacing invalid characters."""
        # Replace invalid filename characters with underscores
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        # Replace spaces with underscores
        filename = filename.replace(' ', '_')
        return filename

    def _get_dataset_dir(self, dataset_type, output_type):
        """Get the appropriate directory for a dataset and output type."""
        if dataset_type not in self.dataset_dirs:
            raise ValueError(f"Unknown dataset type: {dataset_type}. Must be one of {list(self.dataset_dirs.keys())}")
        if output_type not in self.dataset_dirs[dataset_type]:
            raise ValueError(f"Unknown output type: {output_type}. Must be one of {list(self.dataset_dirs[dataset_type].keys())}")
        return self.dataset_dirs[dataset_type][output_type]

    def _get_timestamped_filename(self, base_filename):
        """Add timestamp to filename while preserving extension."""
        base, ext = os.path.splitext(base_filename)
        return f"{base}_{self.timestamp}{ext}"

    def create_summary_plots(self, df, filename_prefix, dataset_type):
        """Create all available plots for the dataframe using the plotter"""
        self.plotter.create_summary_plots(df, filename_prefix, dataset_type)

    def get_dataframe(self, dataset_type):
        """Get the stored DataFrame for a dataset type"""
        return self.dataframes.get(dataset_type)

    def get_plots(self, dataset_type, plot_type=None):
        """Get stored plots for a dataset type and optionally specific plot type from the plotter"""
        return self.plotter.get_plots(dataset_type, plot_type)

    def save_to_db(self, df, dataset_type, table_name=None):
        """Save a DataFrame to the SQLite database with retry logic and store it in the class."""
        # Store the DataFrame
        self.dataframes[dataset_type] = df.copy()

        # Determine table name if not provided
        if table_name is None:
            table_name = 'el_image_data' if dataset_type == 'el' else 'sinton_metadata'

        # Retry settings for DB operations
        max_retries = 3
        retry_delay = 10  # seconds

        for attempt in range(max_retries):
            try:
                result = self.db.create_sqlite_records_from_dataframe(table_name, df)
                print(f"Data saved to database table '{table_name}': {result}")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error saving to database. Retrying in {retry_delay} seconds... Error: {str(e)}")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to save to database after {max_retries} attempts: {str(e)}")

# def save_to_csv(self, df, filename, dataset_type):
#     """Save a DataFrame to a CSV file and store it in the class"""
#     # Store the DataFrame
#     self.dataframes[dataset_type] = df.copy()
#     
#     csv_dir = self._get_dataset_dir(dataset_type, 'csv')
#     timestamped_filename = self._get_timestamped_filename(filename)
#     csv_path = os.path.join(csv_dir, timestamped_filename)
#     
#     # Retry settings for file operations
#     max_retries = 3
#     retry_delay = 10  # seconds
#     
#     for attempt in range(max_retries):
#         try:
#             # Check if file exists and is locked
#             if os.path.exists(csv_path):
#                 try:
#                     # Try to open the file in append mode to check if it's locked
#                     with open(csv_path, 'a'):
#                         pass
#                 except PermissionError:
#                     if attempt < max_retries - 1:
#                         print(f"File {csv_path} is currently in use. Waiting {retry_delay} seconds before retry...")
#                         time.sleep(retry_delay)
#                         continue
#                     else:
#                         # On last attempt, try to save with an additional timestamp
#                         extra_timestamp = datetime.now().strftime("%H%M%S")
#                         base_name, ext = os.path.splitext(timestamped_filename)
#                         new_filename = f"{base_name}_{extra_timestamp}{ext}"
#                         csv_path = os.path.join(csv_dir, new_filename)
#                         print(f"Original file is locked. Saving as {new_filename}")
#             
#             # Save the file
#             df.to_csv(csv_path, index=False)
#             print(f"Data saved to {csv_path}")
#             
#             # Save to database after successful CSV save
#             table_name = 'el_image_data' if dataset_type == 'el' else 'sinton_metadata'
#             self.db.save_dataframe(df, table_name)
#             
#             return
#             
#         except PermissionError as e:
#             if attempt < max_retries - 1:
#                 print(f"Permission denied when saving {csv_path}. Waiting {retry_delay} seconds before retry...")
#                 time.sleep(retry_delay)
#             else:
#                 raise Exception(f"Failed to save CSV after {max_retries} attempts. Last error: {str(e)}")
#         
#         except Exception as e:
#             if attempt < max_retries - 1:
#                 print(f"Error saving CSV. Retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#             else:
#                 raise Exception(f"Failed to save CSV after {max_retries} attempts: {str(e)}")
