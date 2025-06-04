import os
from idp.instrument_data_parser_oo import InstrumentDataParser
from idp.idp_outputer import IDPOutputer
import pandas as pd

def main():
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the folder locations and database path
    folder_locations = [
        os.path.join(current_dir, 'data', 'Sinton_FMT'),
        os.path.join(current_dir, 'data', 'EL_DSLR_CMOS')  
    ]
    sqlite_file_path = os.path.join(os.path.dirname(current_dir), 'PVMCF_Database.db')

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

    # Convert lists to DataFrames for database storage
    if image_metadata:
        image_df = pd.DataFrame(image_metadata)
        print(f"\nImage metadata shape: {image_df.shape}")
        # Drop JPEGThumbnail column
        if 'JPEGThumbnail' in image_df.columns:
            image_df = image_df.drop(columns=['JPEGThumbnail'])
        # Save to CSV
        outputer = IDPOutputer(current_dir)
        outputer.save_to_csv(image_df, 'el_image_data.csv')
    else:
        print("\nNo image metadata found")
        image_df = pd.DataFrame()

    if sinton_metadata:
        sinton_df = pd.DataFrame(sinton_metadata)
        print(f"Sinton metadata shape: {sinton_df.shape}")
        # Save to CSV
        outputer = IDPOutputer(current_dir)
        outputer.save_to_csv(sinton_df, 'sinton_metadata.csv')
    else:
        print("No Sinton metadata found")
        sinton_df = pd.DataFrame()

if __name__ == "__main__":
    main() 