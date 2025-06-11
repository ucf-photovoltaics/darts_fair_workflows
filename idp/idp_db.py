import pandas as pd
import sqlite3 as sq
import os
import datetime

class IDPDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        # Ensure the database file exists (it will be created if it doesn't)
        if not os.path.exists(db_path):
            open(db_path, 'a').close()

    def _log(self, message, error=None):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if error:
            print(f"[{timestamp}] ERROR: {message} | {error}")
        else:
            print(f"[{timestamp}] {message}")

    def save_dataframe(self, df, table_name, if_exists='append', dtype=None):
        """
        Save a pandas DataFrame to the specified table in the SQLite database.
        If the table does not exist, it will be created.
        if_exists: 'append' (default), 'replace', or 'fail'
        dtype: Optional dict of column types (e.g., {'col1': 'TEXT'})
        """
        try:
            with sq.connect(self.db_path) as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False, dtype=dtype)
            self._log(f"Data saved to table '{table_name}' in {self.db_path}")
        except Exception as e:
            self._log(f"Error saving DataFrame to database table '{table_name}'", error=e)

    def insert_records(self, table_name, dataframe):
        """
        Insert new rows to the database for every row in the DataFrame.
        Uses SQL INSERT statements for each row (slower but flexible).
        """
        try:
            with sq.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for _, row in dataframe.iterrows():
                    columns = ', '.join([f'"{col}"' for col in row.index])
                    values = ', '.join([f'?'] * len(row.values))
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                    cursor.execute(sql, tuple(row.values))
                conn.commit()
            self._log(f"Inserted {len(dataframe)} records into '{table_name}'")
        except Exception as e:
            self._log(f"Error inserting records into table '{table_name}'", error=e)

    def read_table(self, table_name, select='*', conditions=None):
        """
        Read records from a table with optional select and conditions.
        Returns a DataFrame.
        """
        try:
            with sq.connect(self.db_path) as conn:
                sql = f"SELECT {select} FROM {table_name}"
                if conditions:
                    sql += f" {conditions}"
                df = pd.read_sql_query(sql, conn)
            self._log(f"Read {len(df)} records from '{table_name}'")
            return df
        except Exception as e:
            self._log(f"Error reading table '{table_name}'", error=e)
            return None

    def execute_query(self, query, params=None):
        """
        Execute a custom SQL query (for advanced use).
        """
        try:
            with sq.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                conn.commit()
            self._log(f"Executed query: {query}")
        except Exception as e:
            self._log(f"Error executing query: {query}", error=e)

    def get_last_date_from_table(self, table_name, date_column='date'):
        """
        Get the last date of a measurement for a table in the database.
        Returns the max value in the date_column.
        """
        try:
            with sq.connect(self.db_path) as conn:
                sql = f"SELECT MAX({date_column}) FROM {table_name}"
                last_date = pd.read_sql_query(sql, conn)
            self._log(f"Got last date from '{table_name}'")
            return last_date.iloc[0, 0]
        except Exception as e:
            self._log(f"Error getting last date from table '{table_name}'", error=e)
            return None
