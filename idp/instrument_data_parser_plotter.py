import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib as mpl
from datetime import datetime

class InstrumentDataParserPlotter:
    """
    Handles all plotting and visualization for instrument data parsing workflows.
    Stores and saves plots for numeric, categorical, correlation, scatter, and time series data.
    """
    def __init__(self, output_dir, timestamp, dataset_dirs):
        """
        Initialize the plotter with output directory, timestamp, and dataset-specific directories.
        Sets up plot storage and configures matplotlib/seaborn styles.
        """
        self.output_dir = output_dir
        self.timestamp = timestamp
        self.dataset_dirs = dataset_dirs
        # Storage for generated plots
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
        # Set plotting style and font
        plt.style.use('seaborn-v0_8')
        sns.set_theme()
        mpl.rcParams['font.family'] = 'DejaVu Sans'
        mpl.rcParams['font.size'] = 10
        mpl.rcParams['axes.titlesize'] = 12
        mpl.rcParams['axes.labelsize'] = 10
        mpl.rcParams['xtick.labelsize'] = 8
        mpl.rcParams['ytick.labelsize'] = 8

    def _sanitize_filename(self, filename):
        """
        Replace invalid filename characters and spaces with underscores.
        """
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        filename = filename.replace(' ', '_')
        return filename

    def _get_dataset_dir(self, dataset_type, output_type):
        """
        Get the directory path for a given dataset and output type.
        Raises ValueError if the type is unknown.
        """
        if dataset_type not in self.dataset_dirs:
            raise ValueError(f"Unknown dataset type: {dataset_type}. Must be one of {list(self.dataset_dirs.keys())}")
        if output_type not in self.dataset_dirs[dataset_type]:
            raise ValueError(f"Unknown output type: {output_type}. Must be one of {list(self.dataset_dirs[dataset_type].keys())}")
        return self.dataset_dirs[dataset_type][output_type]

    def _get_timestamped_filename(self, base_filename):
        """
        Add the class timestamp to a filename, preserving its extension.
        """
        base, ext = os.path.splitext(base_filename)
        return f"{base}_{self.timestamp}{ext}"

    def plot_numeric_columns(self, df, filename_prefix, dataset_type):
        """
        Create and save histograms for all numeric columns in the DataFrame.
        Only output plots for columns with more than one unique value.
        Stores the figures in self.plots.
        """
        plots_dir = self._get_dataset_dir(dataset_type, 'numeric_histograms')
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        self.plots[dataset_type]['numeric_histograms'] = {}
        for col in numeric_cols:
            if df[col].nunique() <= 1:
                print(f"Skipping numeric column '{col}' (only one unique value)")
                continue
            plt.figure(figsize=(10, 6))
            sns.histplot(data=df, x=col, bins=30)
            plt.title(f'Distribution of {col}')
            plt.xticks(rotation=45)
            plt.tight_layout()
            self.plots[dataset_type]['numeric_histograms'][col] = plt.gcf()
            timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_{col}_hist.png')
            plt.savefig(os.path.join(plots_dir, timestamped_filename))
            plt.close()

    def plot_correlation_matrix(self, df, filename_prefix, dataset_type):
        """
        Create and save a correlation matrix heatmap for numeric columns.
        Stores the figure in self.plots.
        """
        plots_dir = self._get_dataset_dir(dataset_type, 'correlations')
        numeric_df = df.select_dtypes(include=['int64', 'float64'])
        if not numeric_df.empty:
            plt.figure(figsize=(12, 8))
            correlation_matrix = numeric_df.corr()
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
            plt.title('Correlation Matrix')
            plt.tight_layout()
            self.plots[dataset_type]['correlations'] = plt.gcf()
            timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_correlation.png')
            plt.savefig(os.path.join(plots_dir, timestamped_filename))
            plt.close()

    def plot_time_series(self, df, date_column, value_column, filename_prefix, dataset_type):
        """
        Create and save a time series plot for a value column against a date column.
        Stores the figure in self.plots.
        """
        plots_dir = self._get_dataset_dir(dataset_type, 'time_series')
        if date_column in df.columns:
            try:
                df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
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
                plt.subplots_adjust(bottom=0.2)
                self.plots[dataset_type]['time_series'] = plt.gcf()
                timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_timeseries.png')
                plt.savefig(os.path.join(plots_dir, timestamped_filename), bbox_inches='tight', dpi=300)
                plt.close()
            except Exception as e:
                print(f"Could not create time series plot: {str(e)}")

    def plot_scatter_matrix(self, df, filename_prefix, dataset_type, columns=None):
        """
        Create and save a scatter matrix (pairplot) for up to 5 numeric columns.
        Stores the figure in self.plots.
        """
        plots_dir = self._get_dataset_dir(dataset_type, 'scatter_matrix')
        if columns is None:
            columns = df.select_dtypes(include=['int64', 'float64']).columns[:5]
        if len(columns) < 2:
            print(f"Not enough numeric columns for scatter matrix in {filename_prefix}. Skipping.")
            return
        pairplot = sns.pairplot(df[columns])
        self.plots[dataset_type]['scatter_matrix'] = pairplot
        timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_scatter_matrix.png')
        pairplot.savefig(os.path.join(plots_dir, timestamped_filename))
        plt.close()

    def plot_categorical_counts(self, df, filename_prefix, dataset_type):
        """
        Create and save bar plots for all categorical columns (object dtype).
        Only output plots for columns with more than one unique value.
        Stores the figures in self.plots.
        """
        plots_dir = self._get_dataset_dir(dataset_type, 'categorical')
        categorical_cols = []
        self.plots[dataset_type]['categorical'] = {}
        for col in df.select_dtypes(include=['object']).columns:
            # Skip columns with array/complex types
            if df[col].apply(lambda x: isinstance(x, (list, tuple, np.ndarray))).any():
                print(f"Skipping column {col} as it contains array data")
                continue
            if df[col].nunique() <= 1:
                print(f"Skipping categorical column '{col}' (only one unique value)")
                continue
            try:
                sample = df[col].astype(str).iloc[0]
                if len(str(sample)) < 100:
                    categorical_cols.append(col)
            except:
                print(f"Skipping column {col} due to data type issues")
                continue
        for col in categorical_cols:
            try:
                plt.figure(figsize=(12, 6))
                df[col] = df[col].astype(str).str.strip()
                value_counts = df[col].value_counts()
                if len(value_counts) > 20:
                    value_counts = value_counts.head(20)
                    plt.title(f'Top 20 Categories in {col}')
                else:
                    plt.title(f'Count of {col}')
                ax = sns.barplot(x=value_counts.index, y=value_counts.values)
                plt.xticks(rotation=45, ha='right')
                for i, v in enumerate(value_counts.values):
                    ax.text(i, v, str(v), ha='center', va='bottom')
                plt.xlabel(col)
                plt.ylabel('Count')
                plt.grid(True, alpha=0.3)
                plt.subplots_adjust(bottom=0.2)
                self.plots[dataset_type]['categorical'][col] = plt.gcf()
                safe_col_name = self._sanitize_filename(col)
                timestamped_filename = self._get_timestamped_filename(f'{filename_prefix}_{safe_col_name}_counts.png')
                plt.savefig(os.path.join(plots_dir, timestamped_filename), bbox_inches='tight', dpi=300)
                plt.close()
            except Exception as e:
                print(f"Error plotting {col}: {str(e)}")
                plt.close()
                continue

    def create_summary_plots(self, df, filename_prefix, dataset_type):
        """
        Create all available summary plots for the DataFrame, including numeric, categorical,
        correlation, scatter (if sinton), and time series (if date/time columns exist).
        """
        print(f"Creating summary plots for {filename_prefix}...")
        self.plot_numeric_columns(df, filename_prefix, dataset_type)
        self.plot_correlation_matrix(df, filename_prefix, dataset_type)
        if dataset_type == 'sinton':
            self.plot_scatter_matrix(df, filename_prefix, dataset_type)
        self.plot_categorical_counts(df, filename_prefix, dataset_type)
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        if date_columns:
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            if not numeric_cols.empty:
                self.plot_time_series(df, date_columns[0], numeric_cols[0], filename_prefix, dataset_type)
        if dataset_type == 'el':
            plots_dir = self._get_dataset_dir(dataset_type, 'numeric_histograms')
        else:
            plots_dir = self._get_dataset_dir(dataset_type, 'scatter_matrix')
        print(f"Summary plots saved to {plots_dir}")

    def get_plots(self, dataset_type, plot_type=None):
        """
        Retrieve stored plots for a dataset type, or a specific plot type if provided.
        """
        if plot_type:
            return self.plots[dataset_type].get(plot_type)
        return self.plots[dataset_type] 