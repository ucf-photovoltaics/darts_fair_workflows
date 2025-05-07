# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 17:41:16 2025

PostgreSQL operations module.

Author: Brent
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from utils import create_logger

# Initialize logger
logger = create_logger()

class PostgresDB:
    def __init__(self, username, password, host="34.73.180.136", port=5432, database="fsecdatabase"):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

    def handle_error(self, error, context):
        """
        Handle errors by logging them.

        Parameters:
        error (Exception): The exception that was raised.
        context (str): A description of the context in which the error occurred.
        """
        logger.error("Error in %s: %s", context, str(error))

    def create_postgres_records_from_dataframe(self, table_name, dataframe):
        """
        Insert new rows into the PostgreSQL table for every row in the DataFrame.

        Parameters:
        table_name (str): Name of the SQL table.
        dataframe (pd.DataFrame): DataFrame containing data to insert.
        """
        try:
            dataframe.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            logger.info("Dataframe records inserted successfully into table %s", table_name)
        except SQLAlchemyError as e:
            self.handle_error(e, "inserting dataframe records")

    def read_records_from_postgres(self, query):
        """
        Fetch data from PostgreSQL using SQLAlchemy and return a DataFrame.

        Parameters:
        query (str): SQL query to execute.

        Returns:
        pd.DataFrame: DataFrame containing the query results or None if an error occurs.
        """
        try:
            df = pd.read_sql(query, self.engine)
            logger.info("Query executed successfully, fetched %d rows", len(df))
            return df
        except SQLAlchemyError as e:
            self.handle_error(e, "fetching data with SQLAlchemy")
            return None

    def fetch_data_by_date(self, table_name, start_date, end_date):
        """
        Fetch records from a specified table where the date is between start_date and end_date.

        Parameters:
        table_name (str): Name of the SQL table.
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.

        Returns:
        pd.DataFrame: DataFrame containing the query results or None if an error occurs.
        """
        query = f"""
        SELECT * FROM {table_name} 
        WHERE date BETWEEN '{start_date}' AND '{end_date}';
        """
        try:
            df = pd.read_sql(query, self.engine)
            logger.info("Query executed successfully, fetched %d rows", len(df))
            return df
        except SQLAlchemyError as e:
            self.handle_error(e, f"fetching data by date from {table_name}")
            return None

    def get_table_names_and_comments(self):
        """
        Connect to PostgreSQL and return a DataFrame with table names and comments.

        Returns:
        pd.DataFrame: DataFrame containing table names and comments or None if an error occurs.
        """
        query = """
           SELECT
               c.relname AS table_name,
               obj_description(c.oid) AS table_comment
           FROM pg_class c
           LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE c.relkind = 'r'
             AND n.nspname NOT IN ('pg_catalog', 'information_schema');
        """
        try:
            df = pd.read_sql(query, self.engine)
            logger.info("Fetched %d tables with comments", len(df))
            return df
        except SQLAlchemyError as e:
            self.handle_error(e, "querying database for table names and comments")
            return None



    def get_table_schema(self, table_name):
        """
        Retrieve the schema of a specified table.

        Parameters:
        table_name (str): Name of the SQL table.

        Returns:
        pd.DataFrame: DataFrame containing the schema details or None if an error occurs.
        """
        query = f"""
           SELECT
               column_name,
               data_type,
               character_maximum_length,
               is_nullable,
               column_default
           FROM information_schema.columns
           WHERE table_name = '{table_name}';
        """
        try:
            df = pd.read_sql(query, self.engine)
            logger.info("Fetched schema for table %s", table_name)
            return df
        except SQLAlchemyError as e:
            self.handle_error(e, f"querying schema for table {table_name}")
            return None

    def __del__(self):
        self.engine.dispose()


# Example usage:
# db = PostgresDB(username="user", password="pass")
# db.create_postgres_records_from_dataframe("table_name", dataframe)