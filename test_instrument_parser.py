import os
from idp.instrument_data_parser_oo import InstrumentDataParser
from idp.instrument_data_parser_outputer import IDPOutputer
import pandas as pd

def main():
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the folder locations and database path dynamically
    folder_locations = [
        os.path.join(current_dir, 'data', 'Sinton_FMT'),
        os.path.join(current_dir, 'data', 'EL_DSLR_CMOS')
    ]
    sqlite_file_path = os.path.join(current_dir, 'database', 'instrument_data.db')

    # Create parser instance
    parser = InstrumentDataParser(folder_locations, sqlite_file_path)

    # Parse image metadata
    print("Parsing image metadata...")
    image_metadata, failed_image_files = parser.parse_image_metadata()
    
    # Parse Sinton FMT metadata
    print("\nParsing Sinton FMT metadata...")
    sinton_metadata, failed_sinton_files = parser.parse_sinton_fmt_metadata()

    # Log parsing results
    parser.log_parsing_results(image_metadata, failed_image_files, sinton_metadata, failed_sinton_files)

    # Create output directory if it doesn't exist
    output_dir = os.path.join(current_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Create outputer instance with the output directory
    outputer = IDPOutputer(output_dir)
    
    # Process image metadata
    if image_metadata:
        print("\nProcessing image metadata...")
        image_df = pd.DataFrame(image_metadata)
        print(f"Image metadata shape: {image_df.shape}")
        
        # Drop JPEGThumbnail column if it exists (it's binary data)
        if 'JPEGThumbnail' in image_df.columns:
            image_df = image_df.drop('JPEGThumbnail', axis=1)
        
        # Save to CSV and create visualizations
        # outputer.save_to_csv(image_df, 'el_image_data.csv', dataset_type='el')
        print("\nCreating visualizations for image metadata...")
        outputer.create_summary_plots(image_df, 'el_image', dataset_type='el')
    else:
        print("\nNo image metadata found")
        image_df = pd.DataFrame()
    
    # Process Sinton metadata
    if sinton_metadata:
        print("\nProcessing Sinton metadata...")
        sinton_df = pd.DataFrame(sinton_metadata)
        print(f"Sinton metadata shape: {sinton_df.shape}")
        
        # Save to CSV and create visualizations
        # outputer.save_to_csv(sinton_df, 'sinton_metadata.csv', dataset_type='sinton')
        print("\nCreating visualizations for Sinton metadata...")
        outputer.create_summary_plots(sinton_df, 'sinton', dataset_type='sinton')
    else:
        print("No Sinton metadata found")
        sinton_df = pd.DataFrame()
    
    # Return the outputer instance for variable explorer access
    return outputer

if __name__ == "__main__":
    # When running directly, store the outputer instance
    outputer = main()
    
    # Assign DataFrames and plots to top-level variables for Spyder variable explorer
    el_df = outputer.get_dataframe('el')
    sinton_df = outputer.get_dataframe('sinton')
    el_plots = outputer.get_plots('el')
    sinton_plots = outputer.get_plots('sinton')
    
    print("\nData processing complete!")
    print("\nTo access the data in Spyder's variable explorer:")
    print("- EL data: el_df")
    print("- Sinton data: sinton_df")
    print("- EL plots: el_plots")
    print("- Sinton plots: sinton_plots") 