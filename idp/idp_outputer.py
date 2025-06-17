import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
import time
import matplotlib as mpl
from .idp_db import IDPDatabase

class IDPOutputer:
    def __init__(self, output_dir=None):
        # Initialize storage for DataFrames and plots
        self.dataframes = {
            'el': None,
            'sinton': None
        }
        self.plots = {
            'el': {
                'numeric_histograms': {},
                'correlations': None,
                'categorical': {},
                'time_series': None
            },
            'sinton': {
                'numeric_histograms': {},
                'correlations': None,
                'scatter_matrix': None,
                'categorical': {},
                'time_series': None
            }
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
                'csv': os.path.join(self.timestamped_dir, 'el', 'csv')
            },
            'sinton': {
                'numeric_histograms': os.path.join(self.timestamped_dir, 'sinton', 'numeric_histograms'),
                'correlations': os.path.join(self.timestamped_dir, 'sinton', 'correlations'),
                'scatter_matrix': os.path.join(self.timestamped_dir, 'sinton', 'scatter_matrix'),
                'categorical': os.path.join(self.timestamped_dir, 'sinton', 'categorical'),
                'time_series': os.path.join(self.timestamped_dir, 'sinton', 'time_series'),
                'csv': os.path.join(self.timestamped_dir, 'sinton', 'csv')
            }
        }
        
        # Create all directories
        for dataset in self.dataset_dirs.values():
            for dir_path in dataset.values():
                os.makedirs(dir_path, exist_ok=True)
        
        # Set style and font
        plt.style.use('seaborn-v0_8')
        sns.set_theme()
        # Use a font that's available on Windows
        mpl.rcParams['font.family'] = 'DejaVu Sans'
        mpl.rcParams['font.size'] = 10
        mpl.rcParams['axes.titlesize'] = 12
        mpl.rcParams['axes.labelsize'] = 10
        mpl.rcParams['xtick.labelsize'] = 8
        mpl.rcParams['ytick.labelsize'] = 8

        # Setup database path
        db_path = os.path.join(self.output_dir, 'database', 'instrument_data.db')
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db = IDPDatabase(db_path)

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

    def save_to_csv(self, df, filename, dataset_type):
        """Save a DataFrame to a CSV file and store it in the class"""
        # Store the DataFrame
        self.dataframes[dataset_type] = df.copy()
        
        csv_dir = self._get_dataset_dir(dataset_type, 'csv')
        timestamped_filename = self._get_timestamped_filename(filename)
        csv_path = os.path.join(csv_dir, timestamped_filename)
        
        # Retry settings for file operations
        max_retries = 3
        retry_delay = 10  # seconds
        
        for attempt in range(max_retries):
            try:
                # Check if file exists and is locked
                if os.path.exists(csv_path):
                    try:
                        # Try to open the file in append mode to check if it's locked
                        with open(csv_path, 'a'):
                            pass
                    except PermissionError:
                        if attempt < max_retries - 1:
                            print(f"File {csv_path} is currently in use. Waiting {retry_delay} seconds before retry...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            # On last attempt, try to save with an additional timestamp
                            extra_timestamp = datetime.now().strftime("%H%M%S")
                            base_name, ext = os.path.splitext(timestamped_filename)
                            new_filename = f"{base_name}_{extra_timestamp}{ext}"
                            csv_path = os.path.join(csv_dir, new_filename)
                            print(f"Original file is locked. Saving as {new_filename}")
                
                # Save the file
                df.to_csv(csv_path, index=False)
                print(f"Data saved to {csv_path}")
                
                # Save to database after successful CSV save
                table_name = 'el_image_data' if dataset_type == 'el' else 'sinton_metadata'
                self.db.save_dataframe(df, table_name)
                
                return
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    print(f"Permission denied when saving {csv_path}. Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to save CSV after {max_retries} attempts. Last error: {str(e)}")
            
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Error saving CSV. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to save CSV after {max_retries} attempts: {str(e)}")

    def plot_numeric_columns(self, df, filename_prefix, dataset_type):
        """Create histograms for all numeric columns and store them"""
        plots_dir = self._get_dataset_dir(dataset_type, 'numeric_histograms')
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        
        # Clear previous histograms
        self.plots[dataset_type]['numeric_histograms'] = {}
        
        for col in numeric_cols:
            plt.figure(figsize=(10, 6))
            sns.histplot(data=df, x=col, bins=30)
            plt.title(f'Distribution of {col}')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Store the figure
            self.plots[dataset_type]['numeric_histograms'][col] = plt.gcf()
            
            # Save to file
            timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_{col}_hist.png')
            plt.savefig(os.path.join(plots_dir, timestamped_filename))
            plt.close()

    def plot_correlation_matrix(self, df, filename_prefix, dataset_type):
        """Create correlation matrix heatmap and store it"""
        plots_dir = self._get_dataset_dir(dataset_type, 'correlations')
        numeric_df = df.select_dtypes(include=['int64', 'float64'])
        if not numeric_df.empty:
            plt.figure(figsize=(12, 8))
            correlation_matrix = numeric_df.corr()
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
            plt.title('Correlation Matrix')
            plt.tight_layout()
            
            # Store the figure
            self.plots[dataset_type]['correlations'] = plt.gcf()
            
            # Save to file
            timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_correlation.png')
            plt.savefig(os.path.join(plots_dir, timestamped_filename))
            plt.close()

    def plot_time_series(self, df, date_column, value_column, filename_prefix, dataset_type):
        """Create time series plot and store it"""
        plots_dir = self._get_dataset_dir(dataset_type, 'time_series')
        if date_column in df.columns:
            try:
                # Convert to datetime if not already, handling potential encoding issues
                df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
                # Drop rows where date conversion failed
                df = df.dropna(subset=[date_column])
                
                if len(df) == 0:
                    print(f"No valid dates found in {date_column}. Skipping time series plot.")
                    return
                
                plt.figure(figsize=(12, 6))
                plt.plot(df[date_column], df[value_column])
                plt.title(f'{value_column} over Time')
                plt.xlabel('Date')
                plt.ylabel(value_column)
                plt.xticks(rotation=45)
                plt.grid(True, alpha=0.3)
                
                # Adjust layout with more space for rotated labels
                plt.subplots_adjust(bottom=0.2)
                
                # Store the figure
                self.plots[dataset_type]['time_series'] = plt.gcf()
                
                # Save to file
                timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_timeseries.png')
                plt.savefig(os.path.join(plots_dir, timestamped_filename), bbox_inches='tight', dpi=300)
                plt.close()
            except Exception as e:
                print(f"Could not create time series plot: {str(e)}")

    def plot_scatter_matrix(self, df, filename_prefix, dataset_type, columns=None):
        """Create scatter matrix and store it"""
        plots_dir = self._get_dataset_dir(dataset_type, 'scatter_matrix')
        if columns is None:
            columns = df.select_dtypes(include=['int64', 'float64']).columns[:5]

        if len(columns) < 2:
            print(f"Not enough numeric columns for scatter matrix in {filename_prefix}. Skipping.")
            return

        # Create and store the pairplot
        pairplot = sns.pairplot(df[columns])
        self.plots[dataset_type]['scatter_matrix'] = pairplot
        
        # Save to file
        timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_scatter_matrix.png')
        pairplot.savefig(os.path.join(plots_dir, timestamped_filename))
        plt.close()

    def plot_categorical_counts(self, df, filename_prefix, dataset_type):
        """Create bar plots for categorical columns and store them"""
        plots_dir = self._get_dataset_dir(dataset_type, 'categorical')
        categorical_cols = []
        
        # Clear previous categorical plots
        self.plots[dataset_type]['categorical'] = {}
        
        for col in df.select_dtypes(include=['object']).columns:
            # Skip columns that contain arrays or complex types
            if df[col].apply(lambda x: isinstance(x, (list, tuple, np.ndarray))).any():
                print(f"Skipping column {col} as it contains array data")
                continue
            # Convert values to strings and check if they're simple categorical data
            try:
                sample = df[col].astype(str).iloc[0]
                if len(str(sample)) < 100:  # Skip very long strings
                    categorical_cols.append(col)
            except:
                print(f"Skipping column {col} due to data type issues")
                continue
        
        for col in categorical_cols:
            try:
                plt.figure(figsize=(12, 6))
                # Convert to string type and clean data
                df[col] = df[col].astype(str).str.strip()
                value_counts = df[col].value_counts()
                
                # Limit to top 20 categories if there are too many
                if len(value_counts) > 20:
                    value_counts = value_counts.head(20)
                    plt.title(f'Top 20 Categories in {col}')
                else:
                    plt.title(f'Count of {col}')
                
                # Create the bar plot with improved styling
                ax = sns.barplot(x=value_counts.index, y=value_counts.values)
                
                # Rotate x-axis labels for better readability
                plt.xticks(rotation=45, ha='right')
                
                # Add value labels on top of bars
                for i, v in enumerate(value_counts.values):
                    ax.text(i, v, str(v), ha='center', va='bottom')
                
                plt.xlabel(col)
                plt.ylabel('Count')
                plt.grid(True, alpha=0.3)
                
                # Adjust layout with more space for rotated labels
                plt.subplots_adjust(bottom=0.2)
                
                # Store the figure
                self.plots[dataset_type]['categorical'][col] = plt.gcf()
                
                # Save to file
                safe_col_name = self._sanitize_filename(col)
                timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_{safe_col_name}_counts.png')
                plt.savefig(os.path.join(plots_dir, timestamped_filename), bbox_inches='tight', dpi=300)
                plt.close()
                
            except Exception as e:
                print(f"Error plotting {col}: {str(e)}")
                plt.close()
                continue

    def create_summary_plots(self, df, filename_prefix, dataset_type):
        """Create all available plots for the dataframe"""
        print(f"Creating summary plots for {filename_prefix}...")
        
        # Basic numeric distributions
        self.plot_numeric_columns(df, filename_prefix, dataset_type)
        
        # Correlation matrix
        self.plot_correlation_matrix(df, filename_prefix, dataset_type)
        
        # Dataset-specific visualizations
        if dataset_type == 'sinton':
            # Scatter matrix only for Sinton data
            self.plot_scatter_matrix(df, filename_prefix, dataset_type)
        
        # Categorical counts
        self.plot_categorical_counts(df, filename_prefix, dataset_type)
        
        # Time series if date column exists
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if date_columns:
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            if not numeric_cols.empty:
                self.plot_time_series(df, date_columns[0], numeric_cols[0], filename_prefix, dataset_type)
        
        # Print summary of where plots were saved
        if dataset_type == 'el':
            plots_dir = self._get_dataset_dir(dataset_type, 'numeric_histograms')
        else:  # sinton
            plots_dir = self._get_dataset_dir(dataset_type, 'scatter_matrix')
        print(f"Summary plots saved to {plots_dir}")

    def get_dataframe(self, dataset_type):
        """Get the stored DataFrame for a dataset type"""
        return self.dataframes.get(dataset_type)

    def get_plots(self, dataset_type, plot_type=None):
        """Get stored plots for a dataset type and optionally specific plot type"""
        if plot_type:
            return self.plots[dataset_type].get(plot_type)
        return self.plots[dataset_type]
