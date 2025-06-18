"""
Created on Wed May  8 12:10:54 2024

@author: Brent Thompson, Dylan Colvin

This module provides functions for managing the PVMCF text databases.

Key functionalities include:
* Reading database files
* Creating module IDs
* Copying module information
* Adding new entries
* Updating existing entries
* Saving database changes

The module interacts with LabVIEW to perform basic CRUD database operations
based on command-line arguments sent from LabVIEW VIs.
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime
import logging
import sqlite3 as sq
"""
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
"""
MODULES = 'E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/module_databases/module-metadata.txt'
#MODULES = 'C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/module-metadata.txt'

main_dir = "E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/General/FSEC_PVMCF/"

#main_dir = "C:/FSEC/Databases/"

DATASETS = f"{main_dir}module_databases/"
database = DATASETS + "PVMCF_Database.db"
#database =  'C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/FSEC_Database.db'
database_log = DATASETS + "PVMCF_Database_log.log"



def get_date():  # Get the current date in YYMM format
    """
    Returns the current date in YYMM format.

    Returns:
        str: The current date in YYMM format.
    """
    today = datetime.now()
    today = datetime.strftime(today, '%y%m')
    return today


def read_database(path=None):  # Open the database to be used
    """
    Reads a database file in CSV, Excel, or text format.

    Args:
        path (str, optional): The path to the database file. If None,
        prompts the user to select a file.

    Returns:
        pandas.DataFrame: The loaded database as a DataFrame.
    """
    if path is None:
        exit

    if path.endswith('.txt') or path.endswith('.csv'):
        try:
            database = pd.read_csv(path, sep=',', encoding='latin-1',
                                   dtype='str', error_bad_lines=False)
        except TypeError:  # Throws TypeError when 3.7 code is ran on 3.12
            database = pd.read_csv(path, sep=',', encoding='latin-1',
                                   dtype='str')
    elif path.endswith('.xlsx') or path.endswith('.xls'):
        database = pd.read_excel(path, dtype='str')
    else:
        print("You did not select the proper database.")

    return database


def create_module_id(database):  # Creates Module ID for use with add_entry
    """
    Creates a new module ID based on the contents of the database.

    This function generates a unique module ID with format "FYYMM-####" where:
    - F: is a fixed prefix
    - YYMM: represents the current year and month
    - ####: is a sequential number

    The function filters out control modules (VCAD, PSEL) and increments the
    sequential number based on the most recent FSEC-ID in the database.

    Args:
        database (pandas.DataFrame): Database containing module information.

    Returns:
        str: The newly generated module ID.
    """
    today = get_date()

# Selects the most recent FSEC-ID with the standard FYYMM format
    database['module-id'] = database['module-id'].astype(str)
    fsec_id_field = database[['module-id']]

    fsec_id_field = [
        entry for entry in fsec_id_field['module-id'] if (
            entry[0] == "F") and (entry[1] != "P") and (entry[1] != "9")]

    most_recent_id = sorted(fsec_id_field)[-1]

# Strip prefix and suffix for processing
    fsec_prefix = most_recent_id.split('-')[0].strip('F')
    fsec_suffix = int(most_recent_id.split('-')[1]) + 1
    fsec_suffix = (f'{fsec_suffix:04}')

# Run check to determine module id, create the id and alert user
    new_module_id = (f"F{today}-0001")
    if today == fsec_prefix:
        new_module_id = (f"F{today}-{fsec_suffix}")
        print(f'{most_recent_id} found in database, {new_module_id} created.')

    return new_module_id


def get_model_info(database, model_number):
    """
    Copies information from an existing model to a new row in the database.

    This function creates a new row based on the most recent occurrence of the
    specified model number. The new row's `module-id` is set to `new_module_id`
    (assumed to be defined in the calling scope). The `serial-number` column is
    cleared if it exists in the database.

    Args:
        database (pandas.DataFrame): Database containing module information.
        model_number (str): The model number to copy information from.

    Returns:
        pandas.DataFrame: The updated database with the new row.

    Raises:
        IndexError: If no model matching the specified model number is found.
    """
    try:
        new_row = database.loc[database['model'] == model_number]
        new_row = pd.DataFrame([new_row.iloc[-1]])
        new_row.loc[:, 'module-id'] = new_module_id

        if 'serial-number' in database:
            new_row.loc[:, 'serial-number'] = ''
        database = pd.concat([database, new_row], ignore_index=True)

    except IndexError:
        print(f'No model exists that matches {model_number}, use manual add.')
        sys.exit()

    return database


def add_serial_number(database, serial_number, new_module_id):
    """
    Adds a serial number to the specified module in the database.

    If the module ID exists, updates the serial number for that module.
    Otherwise, adds the serial number to the last entry in the database.

    Args:
        database (pandas.DataFrame): Database containing module information.
        serial_number (str): The serial number to add.
        new_module_id (str): The module ID to associate with the serial number.

    Returns:
        pandas.DataFrame: The updated database with the added serial number.
    """
    if not serial_number:
        print("No serial number entered.")
        sys.exit()
    else:
        try:
            idx = database[database['module-id'] == new_module_id].index[0]
            print('Try triggered for add serial number')

        except ValueError:
            idx = len(database) - 1
            print('Except triggered for add serial number')

        database.loc[idx, 'serial-number'] = serial_number
        print(f'{serial_number} added to {new_module_id}')

    return database


def add_new_entry(database, new_values, parameters):
    """
    Adds a new row to the database with the specified values and parameters.

    This function creates a new row at the end of the database and populates it
    with the provided values and parameters. It also adds the `new_module_id`
    to the 'module-id' column if it exists in the database.

    Args:
        database (pandas.DataFrame): Database to add the new entry to.
        new_values (list): Values to populate the new row.
        parameters (list): Column names corresponding to the new values.

    Returns:
        pandas.DataFrame: The updated database with the new row.
    """
    idx = len(database)
    database.loc[idx] = None

# Update values for new entry in database on each key
    for key, value in zip(parameters, new_values):
        database.loc[idx, key] = value.upper()

    if 'module-id' in database:
        database.loc[idx, 'module-id'] = new_module_id
    else:
        pass

    return database


def update_entry(database, new_values, parameters):
    """
    Updates an existing entry in the database with
    the specified values and parameters.

    This function finds the row with the matching `new_module_id` and
    updates the specified columns with the provided values.

    Args:
        database (pandas.DataFrame): The database to update.
        new_values (list): A list of values to update the entry with.
        parameters (list): A list of column names corresponding to the values.

    Raises:
        IndexError: If the module with `new_module_id` is not found.

    Returns:
        pandas.DataFrame: The updated database.
    """
    try:
        idx = database[database['module-id'] == new_module_id].index[0]
        # Update values for entry in database on each key
        for key, value in zip(parameters, new_values):
            database.loc[idx, key] = value.upper()

    except IndexError:
        print('Module not found in database.')
        sys.exit()

    return database


def save_database(database, file_path):
    """
    Saves database to the specified file path in both CSV and Excel formats.

    Args:
        database (pandas.DataFrame): The database to save.

    file_path (str): The base file path for saving the database. The function
        will create both a TXT and SQL update with the same base name.
    """
    #ext = file_path.split('.')[-1]
    database = database.astype(str)
    database.to_csv(file_path, sep='\t', index=False)
    """
    table_name = basename(file_path).replace(f".{ext}", '').replace('-','_')
    file_name = file_path.split('/')[-1]
    db_file_path = file_path.replace(file_name, 'FSEC_Database.db')
    conn = sq.connect(db_file_path)
    database.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    """
    return print(f'txt file database saved at {file_path}.')


def join_module_metadata(dataframe):
    """
    This function can be used to join the Make and Model from module metadata
    reducing the room for human error and keeping everything consistant.

    The only input is a dataframe that has serial numbers as a column

    If the serial numbers match, the make and model will replace exisiting.
    """
    modules = pd.read_csv(MODULES, sep='\t', usecols=[
                          "module-id", "make", "model", "serial-number"])
    dataframe = dataframe.merge(
        modules, how='left', left_on="serial-number", right_on="serial-number")
    try:
        dataframe = dataframe.drop(columns={'make_y', 'model_y'})
        dataframe = dataframe.rename(
            columns={'make_x': 'make', 'model_x': 'model'})
    except:
        print("There were no existing columns named make and model")
    return dataframe

## SQL Functions begin here. 
def get_last_date_from_table(table_name='sinton-iv-metadata'):
    """
    Gets the last date of a measurement for a table in the database. Used to 
    determine which folders need to be added for processing.

    Parameters
    ----------
    table_name : String
        Name of the table in the sqlite database to search in

    Returns
    -------
    Integer value of last date in YYYYMMDD format

    """

    with sq.connect(database) as connection:
        cursor = connection.cursor()
    sql = f"SELECT MAX(date) from '{table_name}'"

    try:
        # Execute and commit the SQL
        cursor.execute(sql)
        last_date = pd.read_sql_query(sql, connection)
        connection.close()
    except Exception:
        print("Problem with finding last date.")

    return last_date.loc[0][0]


def deserialize_array(blob, dtype=np.float64):  # Adjust dtype as needed
    """
    Used to deserialize the arrays that are encoded during the storage of 
    IV array data to database. Used to recover voc array, isc array, corrected
    results and interpolated data. 

    Parameters
    ----------
    blob : TYPE
        This blob is the result of serialization that occurs during parsing.
    dtype : TYPE, optional
        DESCRIPTION. The default is np.float64.

    Returns
    -------
    None.

    """
    return np.frombuffer(blob, dtype=dtype)


def create_sqlite_record(table_name, columns, values):
    """
    Inserts a single new entry to the database

    Parameters
    ----------
    table_name : String
        Name of SQL table to insert data to
        Single quotes surround table name in double quotes '"table-name"'
    columns : List
        List of column names for respective table name.
    values : List
        List of values to enter on column names

    """
    with sq.connect(database) as connection:
        cursor = connection.cursor()

    # Generate SQL for inserting data
    columns = ', '.join(columns)
    values = ', '.join(values)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    try:
        # Execute and commit the SQL
        cursor.execute(sql)
        connection.commit()
    except Exception as e:

        print("Problem with SQL command: " + sql)
        return e

    connection.close()
    return "Entry added to " + table_name


def create_sqlite_records_from_dataframe(table_name, dataframe):
    """
    Inserts a new row to the database for every row in dataframe

    Parameters
    ----------
    table_name : String
        Name of SQL table to insert data to
        Single quotes surround table name in double quotes '"table-name"'.
    dataframe : Pandas Dataframe
        Output from metadata parsing modules, used as raw data to construct
        SQL statements.

    """
    with sq.connect(database) as connection:
        cursor = connection.cursor()

    # Generate SQL for inserting data
    for value, row in dataframe.iterrows():
        space = ' ', ''
        columns = ', '.join(
            [f'"{col.replace(space[0],space[1])}"' for col in row.index])
        placeholders = ', '.join([f'"{value}"' for value in row.values])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            # Execute and commit the SQL
            cursor.execute(sql)
            connection.commit()
        except Exception:
            # print(str(row[0]) + " Was not added to " + table_name)
            pass

    connection.close()
    return table_name + " Updated With " + str(len(dataframe)) + " Entries."


# Function to read data from a table
def read_records(database, table_name, select='*', conditions=None):
    """
    Returns the contents of a table as a dataframe.

    Parameters
    ----------
    database: String of path to sqlite database
    table_name : String
        Name of SQL table to insert data to
        Single quotes surround table name in double quotes '"table-name"'.
    select : String, optional
        Choose which columns to select, default is all
    conditions : String, optional
        WHERE, GROUP BY, ect..

    Returns
    -------
    records : Pandas Datafrane
        Results of SQL query in dataframe.

    """
    with  sq.connect(database) as connection:
        cursor = connection.cursor()

    # Build SQL query, optional conditions for complex queries
    sql = f"SELECT {select} FROM {table_name}"
    if conditions:
        sql += f" {conditions}"

    # Execute the query and fetch all records
    cursor.execute(sql)
    records = pd.read_sql_query(sql, connection)
    connection.close()
    return records


def create_logger():
    """
    Sets up and configures a logger to keep track of errors and system events.

    Returns
    -------
    logger : Logger Object
        Records events during runtime in log file PVMCF_Database_log.log.

    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(database_log)
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(file_handler)
    logger.info("Main update database program started.")

    return logger


def blank_insert_to_database(table_name, dataframe):
    """
    This function can be used as a fallback in automated systems to ensure
    data is saved to a table even if something unexpected changes
    with the data. 
    
    There is no schema or failsafes with this method. Best of luck, truely.

    Should only be called during exception handling or one of inserts to the
    database
    """
    with sq.connect(database) as connection:

        dataframe.to_sql(
            f'{table_name}', connection, if_exists='append', index='false',
            dtype={col_name: 'TEXT' for col_name in dataframe})
 

def connect_to_postgres():
    connection = psycopg2.connect(
        database="fsecdatabase", user="brenthom", password="Solar2025", host="34.73.180.136", port=5432)
    cursor = connection.cursor()
    cursor.execute("SELECT * from 'fsecdatabase.module_metadata';")


def create_postgres_records_from_dataframeold(table_name, dataframe):
    
    with psycopg2.connect(
            database="fsecdatabase", user="brenthom", password="Solar2025", host="34.73.180.136", port=5432) as connection:
        cursor = connection.cursor()

    # Generate SQL for inserting data
    for value, row in dataframe.iterrows():
        space = ' ', ''
        columns = ', '.join(
            [f'"{col.replace(space[0],space[1])}"' for col in row.index])
        placeholders = ', '.join([f'"{value}"' for value in row.values])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            # Execute and commit the SQL
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            print(e)
            print(str(row[0]) + " Was not added to " + table_name)
            pass

    connection.close()
    return table_name + " Updated With " + str(len(dataframe)) + " Entries."


def create_postgres_records_from_dataframe(table_name, dataframe):
    """
    Inserts new rows into the table for every row in the dataframe.
    
    Parameters:
        table_name (str): Name of the SQL table.
        dataframe (pd.DataFrame): DataFrame containing data to insert.
    """
    engine = create_engine("postgresql://brenthom:Solar2025@34.73.180.136:5432/fsecdatabase")
    
    # It is best to use pandas' built-in to_sql with SQLAlchemy engine.
    try:
        dataframe.to_sql(
            name=table_name,   # Replace with your actual table name
            con=engine,
            if_exists='append',       # or 'replace' or 'fail'
            index=False,
            method='multi'            # Optional: allows multi-row insert for efficiency
            )
        #return f"{table_name} updated with {len(dataframe)} entries."
    except Exception as e:
        logging.error("Error inserting dataframe records: %s", e)
        #return e

def read_records_from_postgres(username, password, query):
    """Fetches data using SQLAlchemy and returns a pandas DataFrame."""
    try:
        engine = create_engine(f"postgresql://{username}:{password}@34.73.180.136:5432/fsecdatabase")
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print("Error fetching data with SQLAlchemy:", e)
    finally:
        engine.dispose()
        


def fetch_data_by_date(username, password, start_date, end_date):
    """Fetch records from elmetadata where date_column is between start_date and end_date."""
    try:
        # Establish connection
        engine = create_engine(f"postgresql://{username}:{password}@34.73.180.136:5432/fsecdatabase")
        
        # Define SQL query with date filter (Assuming the date column is named 'date_column')
        query = f"""
        SELECT * FROM el_metadata 
        WHERE date BETWEEN '{start_date}' AND '{end_date}';
        """
        
        # Fetch data into DataFrame
        df = pd.read_sql(query, engine)
        
        return df
    except Exception as e:
        print("Error fetching data:", e)
    finally:
        engine.dispose()
        return None
    
    
def get_table_names_and_comments(username, password):
    """
    Connects to a PostgreSQL database and returns a list of dictionaries,
    each containing the table name and its associated comment.
    """
    # Create the SQLAlchemy engine for PostgreSQL.
    engine = create_engine(f"postgresql://{username}:{password}@34.73.180.136:5432/fsecdatabase")
    
    # SQL query to get table names and comments.
    query = text("""
       SELECT
           c.relname AS table_name,
           obj_description(c.oid) AS table_comment
       FROM pg_class c
       LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'r'
         AND n.nspname NOT IN ('pg_catalog', 'information_schema');
   """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            rows = result.fetchall()
            
            # Convert each row to a dictionary.
            tables = [
                {"table_name": row[0], "table_comment": row[1]}
                for row in rows
            ]
            return tables
    except SQLAlchemyError as e:
        print("Error querying database:", e)
    finally:
        engine.dispose()
        

##########################
# Start of Main Program Initiated by LabVIEW command line arguements  #
if __name__ == "__main__":
    print("Main program started...make sure files are closed")
    """
    Main entry point for the photovoltaic module database management script.

    This script interacts with LabVIEW to perform various operations on
    PVMCF text databases.

    Args:
        sys.argv: Command-line arguments.

    """

    database_root = "E:/University of Central Florida/UCF_Photovoltaics_GRP - Documents/"
    database_root = f"{database_root}General/FSEC_PVMCF/module_databases/"

    database = database_root + "PVMCF_Database.db"
    database_log = database.replace(".db","_log.log")
    logger = create_logger()

    module_filepath = f"{database_root}module-metadata.txt"
    settings_filepath = f"{database_root}measurement-settings.txt"
    status_filepath = f"{database_root}module-status.txt"

    module_database = read_database(module_filepath)
    
    
    measurement_settings = read_records(database, "'measurement-settings'")
    
    module_metadata = read_records(database, "'module-metadata'")
    previous_module_id = module_metadata[['module-id']].iloc[-1][0]
    
    
    settings_database = read_database(settings_filepath)
    status_database = read_database(status_filepath)

    command = sys.argv[1].replace('"', '')

    new_values = sys.argv[2].replace('"', '')
    new_values = [value for value in new_values.split(' ')]

    parameters = sys.argv[3].replace('"', '')
    parameters = [parameter for parameter in parameters.split(' ')]

    new_module_id = create_module_id(module_metadata)

    if len(sys.argv) > 4:
        new_module_id = sys.argv[4]
        print(f'{new_module_id} loaded from Labview')

# Branch depending on sys.argv[1] from LabVIEW

    if command == 'add_new_entry':
        print("Add new entry started.")
        try:
            module_database = add_new_entry(
                module_database, new_values, parameters)

            save_database(module_database, module_filepath)
        except ValueError:
            print('Error adding new entry. Run manual addition.')    
            pass
        try:
            create_sqlite_record("'module-metadata'", 
                                 parameters.prepend('module-id'), 
                                 new_values.prepend(new_module_id))
        except:
            pass
        print("Add new entry finished.")

    elif command == 'copy_module_information':
        print("Copy module information started.")
        model_number = sys.argv[2]

        module_database = get_model_info(module_database, model_number)
        settings_database = get_model_info(settings_database, model_number)

        try:
            if model_number in module_metadata[['model']]:
                new_row = module_metadata.loc[module_metadata['model'] == model_number]
                create_sqlite_records_from_dataframe("'module-metadata'", new_row)
        except:
            pass
        

        try:
            if model_number in measurement_settings[['model']]:
                new_row = measurement_settings.loc[measurement_settings['model'] == model_number]
                create_sqlite_records_from_dataframe("'measurement-settings'", new_row)
        except:
            pass

        save_database(module_database, module_filepath)
        save_database(settings_database, settings_filepath)

          
        print("Copy module information finished")

    elif command == 'write_measurement_settings':
        print("Write measurement setting started.")

        if not np.isin(new_module_id, settings_database['module-id']):
            settings_database = add_new_entry(settings_database,
                                              new_values=[new_module_id],
                                              parameters=['module-id'])
            try:
                create_sqlite_record("'measurement-settings'", 
                             parameters.prepend('module-id'), 
                             new_values.prepend(new_module_id))
            except:
                pass

        settings_database = update_entry(
            settings_database, new_values, parameters)

        save_database(settings_database, settings_filepath)
        print("Write measurement settings finished")

    elif command == 'add_serial_number':
        print("Adding serial number")
        serial_number = sys.argv[2]
        print(f'{serial_number} sent from Labview to python')
        add_serial_number(module_database, serial_number, new_module_id)
        save_database(module_database, module_filepath)
        print(f'{serial_number} added')

    elif command == 'write_status_event':
        add_new_entry(status_database, new_values, parameters)
        save_database(status_database, status_filepath)

    elif command == 'create_module_id':
        new_module_id = create_module_id(module_database)
        print(f'{new_module_id} created')
    else:
        print('Something went wrong... Script did not run properly')

    print('Script finished running, check that data was added correctly')

# End of Main Program #
##########################
