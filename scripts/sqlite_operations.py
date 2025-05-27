# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 17:51:57 2025

@author: Doing
"""

import pandas as pd
import sqlite3 as sq
import os
from utils import create_logger

# Configuration
DATABASE_PATH = os.getenv("DATABASE_PATH", "C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/FSEC_Database.db")

# Initialize logger
logger = create_logger()

class SQLiteDB:
    def __init__(self, database_path=DATABASE_PATH):
        self.database_path = database_path

    def handle_error(self, error, context):
        """
        Handle errors by logging them.

        Parameters:
        error (Exception): The exception that was raised.
        context (str): A description of the context in which the error occurred.
        """
        logger.error("Error in %s: %s", context, str(error))

    def read_records(self, table_name, select='*', conditions=None):
        """
        Return the contents of a table as a DataFrame.

        Parameters:
        table_name (str): Name of the SQL table.
        select (str): Columns to select.
        conditions (str): SQL conditions.

        Returns:
        pd.DataFrame: DataFrame containing the query results.
        """
        try:
            with sq.connect(self.database_path) as connection:
                sql = f"SELECT {select} FROM {table_name}"
                if conditions:
                    sql += f" {conditions}"
                records = pd.read_sql_query(sql, connection)
                return records
        except Exception as e:
            self.handle_error(e, "reading records from table")
            return None

    def blank_insert_to_database(self, table_name, dataframe):
        """
        Fallback function to save data to a table even if data format changes.

        Parameters:
        table_name (str): Name of the SQL table.
        dataframe (pd.DataFrame): DataFrame containing data to insert.
        """
        try:
            with sq.connect(self.database_path) as connection:
                dataframe.to_sql(table_name, connection, if_exists='append', index=False, dtype={col: 'TEXT' for col in dataframe})
                
        except Exception as e:
            self.handle_error(e, "inserting data into table")

    def create_sqlite_record(self, table_name, columns, values):
        """
        Insert a single new entry to the database.

        Parameters:
        table_name (str): Name of the SQL table.
        columns (list): List of column names.
        values (list): List of values to insert.

        Returns:
        str: Success message or error.
        """
        try:
            with sq.connect(self.database_path) as connection:
                cursor = connection.cursor()
                columns_str = ', '.join(columns)
                values_str = ', '.join([f"'{val}'" for val in values])
                sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
                cursor.execute(sql)
                connection.commit()
                logger.info("Records inserted successfully into table %s", table_name)
                return "Entry added to " + table_name
            
        except Exception as e:
            self.handle_error(e, "creating SQLite record")
            return str(e)

    def create_sqlite_records_from_dataframe(self, table_name, dataframe):
        """
        Insert new rows to the database for every row in the DataFrame.

        Parameters:
        table_name (str): Name of the SQL table.
        dataframe (pd.DataFrame): DataFrame containing data to insert.

        Returns:
        str: Success message.
        """
        try:
            with sq.connect(self.database_path) as connection:
                for _, row in dataframe.iterrows():
                    columns = ', '.join([f'"{col}"' for col in row.index])
                    values = ', '.join([f'"{val}"' for val in row.values])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                    cursor = connection.cursor()
                    cursor.execute(sql)
                    connection.commit()
                logger.info("Records inserted successfully into table %s", table_name)
            return f"{table_name} updated with {len(dataframe)} entries."
        except Exception as e:
            self.handle_error(e, "creating SQLite records from dataframe")
            return str(e)

    def join_module_metadata(self, dataframe):
        """
        Join the Make and Model from module metadata, reducing human error and maintaining consistency.

        Parameters:
        dataframe (pd.DataFrame): DataFrame with serial numbers as a column.

        Returns:
        pd.DataFrame: Updated DataFrame with joined metadata.
        """
        query = """
            SELECT "module-id","make","model","serial-number"
            FROM "module-metadata";
        """

        try:
            with sq.connect(self.database_path) as connection:
                modules = pd.read_sql_query(query, connection)
            dataframe = dataframe.merge(modules, how='left', left_on="serial_number", right_on="serial-number")
            dataframe.drop(columns=['make_y', 'model_y'], inplace=True, errors='ignore')
            dataframe.rename(columns={'make_x': 'make', 'model_x': 'model'}, inplace=True)
            return dataframe
        except Exception as e:
            self.handle_error(e, "joining module metadata")
            return dataframe

    def get_last_date_from_table(self, table_name='sinton-iv-metadata'):
        """
        Get the last date of a measurement for a table in the database.

        Parameters:
        table_name (str): Name of the table in the SQLite database.

        Returns:
        int: Last date in YYYYMMDD format.
        """
        try:
            with sq.connect(self.database_path) as connection:
                sql = f"SELECT MAX(date) from '{table_name}'"
                last_date = pd.read_sql_query(sql, connection)
            return last_date.loc[0][0]
        except Exception as e:
            self.handle_error(e, "getting last date from table")
            return None

# Example usage:
# db = SQLiteDB()
# db.read_records("table_name")