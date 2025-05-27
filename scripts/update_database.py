# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 16:44:56 2024

@authors: Brent Thompson


Update Database: Software for updating databases at Florida Solar Energy Center PVMCF 1940-101.

Metadata is processed from the following systems:
    - Module Metadata, Measurement Settings, Module Status
    - Sinton Measurement Metadata (MFR)
    - Dark IV (DIV)
    - Module Scanner Metadata (NC)
    - V10 Metadata (V10)
    - IR Metadata (IR)
    - UVF Metadata(UVF)


The program is designed to be run on a daily basis, and can be ran whenever new data is available.
This program will use current FSEC filename standards and update FSEC database with any new data. 

Events and Errors are tracked and logged in 'database_log.log'.

"""
import database_manipulation as dm
#import instrument_data_parser

import mfr_pipeline
import darkiv_pipeline
import el_pipeline
import ir_indoor_pipeline
import uvf_indoor_pipeline

main_dir = "E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/"

#main_dir = "C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/FSEC_Database.db"


def new_EOD_Update_FSEC_Database():
    """
    
    This routine fully integrates with the sql database. 
    
    """
    DATABASE = f"{main_dir}/PVMCF_Database.db"

    LOG = f"{main_dir}module_databases/PVMCF_Database_log.log"
    
    
    
    

def old_EOD_Update_FSEC_Database(logger):
    """
    This routine goes through each dataset and updates the FSEC database

    Each dataset has an original source in Instrument Data

    """
    DATASETS = f"{main_dir}module_databases/"
    database = DATASETS + "PVMCF_Database.db"
    database_log = DATASETS + "PVMCF_Database_log.log"
# Module Metadata
    
    module_metadata = dm.read_database(
        f'{DATASETS}module-metadata.txt')
    log = dm.create_sqlite_records_from_dataframe(
        '"module-metadata"', module_metadata)
    logger.info(log)

# Measurement Settings
    measurement_settings = dm.read_database(
        f'{DATASETS}measurement-settings.txt')
    log = dm.create_sqlite_records_from_dataframe(
        '"measurement-settings"', measurement_settings)
    logger.info(log)

# Module Status - Needs primary key
    """
    module_status = dm.read_database(
        f'{DATASETS}module-status.txt')
    log = dm.create_sqlite_records_from_dataframe(
        '"module-status"', module_status)
    logger.info(log)
    """
    
# Sinton IV
    last_sinton_id = dm.read_records(
        database, '"sinton-iv-results"', select="ID", conditions="").iloc[-1][0]
    sinton_iv = dm.read_records(
        "C:/SintonInstruments/Database/MultiFlash/MultiFlash_Database.s3db",
        "Results", conditions=f"WHERE ID > {last_sinton_id}")
    log = dm.create_sqlite_records_from_dataframe(
        '"sinton-iv-results"', sinton_iv)
    logger.info(log)

# Sinton MFR

    sinton_mfr = mfr_pipeline.mfr_database_updater()
    log = dm.create_sqlite_records_from_dataframe(
        '"sinton-iv-metadata"', sinton_mfr)
    logger.info(log)

# DSLR CMOS EL

    cmos_el = el_pipeline.el_database_updater()
    log = dm.create_sqlite_records_from_dataframe('"el-metadata"', cmos_el)
    logger.info(log)

# Dark IV
    dark_iv = darkiv_pipeline.dark_iv_database_updater()
    #dark_iv = mp.parse_darkiv_metadata()

    log = dm.create_sqlite_records_from_dataframe('"dark-iv-metadata"', dark_iv)
    logger.info(log)

# Indoor IR
    indoor_ir = ir_indoor_pipeline.indoor_ir_database_updater()
    log = dm.create_sqlite_records_from_dataframe(
        '"ir-indoor-metadata"', indoor_ir)
    logger.info(log)


# Indoor UVF
    indoor_uvf = uvf_indoor_pipeline.indoor_uvf_database_updater()
    log = dm.create_sqlite_records_from_dataframe(
        '"uvf-indoor-metadata"', indoor_uvf)
    logger.info(log)

# V10
    # TODO Fix Error
    """
    v10_data = mp.parse_v10_metadata()
    log = dm.create_sqlite_records_from_dataframe('"v10-metadata"', v10_data)
    logger.info(log)
  

# Module Scanner
    # TODO only select the most recent scanner files

    scanner_nc = dm.read_database(f'{DATASETS}scanner-nc-metadata.txt')
    log = dm.create_sqlite_records_from_dataframe(
        '"scanner-nc-metadata"', scanner_nc)
    logger.info(log)

    scanner_jpg = dm.read_database(
        f'{DATASETS}scanner-jpg-metadata.txt')
    log = dm.create_sqlite_records_from_dataframe(
        '"scanner-jpg-metadata"', scanner_jpg)
    logger.info(log)
    """


# Properly shutdown logging
    logger.info("FSEC Database Update Complete.")
    for handler in logger.handlers:
        handler.flush()
    dm.logging.shutdown()

    return 0


if __name__ == "__main__":
    logger = dm.create_logger()
    old_EOD_Update_FSEC_Database(logger)
