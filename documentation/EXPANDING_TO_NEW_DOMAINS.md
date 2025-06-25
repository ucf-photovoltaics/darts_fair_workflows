# Expanding the Instrument Data Parser Workflow to New Domains

Welcome! This guide will help you adapt the instrument data parser workflow for your own research data, even if you have little or no experience with Python or databases. The workflow is modular and designed to be easy to extend.

---

## 1. Workflow Overview

The workflow is made up of four main parts:
- **Parser**: Reads and processes your raw data files.
- **Outputer**: Handles saving results, making plots, and writing to the database.
- **SQLiteDB**: Manages all database operations.
- **Plotter**: (Optional) Makes visualizations from your data.

See the diagram in `documentation/Expanding_to_New_Domains.svg` for a visual overview.

---

## 2A. Option 1: Creating Your Own Data Parser Class

### Step 1: Copy and Rename the Parser Class
- Copy `idp/instrument_data_parser_oo.py` to a new file, e.g. `idp/my_custom_parser.py`.
- Rename the class (e.g. `MyCustomParser`).
- Update the logic in the class to read your data format (CSV, Excel, images, etc.).

**Example:**
```python
class MyCustomParser:
    def __init__(self, folder_locations, db_path):
        self.folder_locations = folder_locations
        self.db_path = db_path
    def parse_metadata(self):
        # Your code to read and process files
        return metadata_list, failed_files
```

### Step 2: Connect to the Outputer
- Use or make a subclass of `InstrumentDataParserOutputer` (see `idp/instrument_data_parser_outputer.py`).
- Pass your DataFrame(s) to the outputer for saving, plotting, and database storage.

**Example:**
```python
outputer = InstrumentDataParserOutputer(output_dir)
outputer.save_to_db(my_df, dataset_type='mydata')
outputer.create_summary_plots(my_df, 'mydata', dataset_type='mydata')
```

### Step 3: Customize Output and Plots
- Add or modify plotting functions in the outputer or plotter classes.
- Save to custom database tables by specifying `table_name` in `save_to_db`.

**Example:**
```python
outputer.save_to_db(my_df, dataset_type='mydata', table_name='my_custom_table')
```

### Step 4: Use the SQLiteDB Class
- The `SQLiteDB` class (in `scripts/sqlite_operations.py`) provides easy methods to read/write tables.
- You can use it directly if you want to do custom queries.

**Example:**
```python
from scripts.sqlite_operations import SQLiteDB
db = SQLiteDB('output/database/instrument_data.db')
df = db.read_records('my_custom_table')
```

### Step 5: Run and Test Your Workflow
- Create a test script (see `test_instrument_parser.py` for an example).
- Run your script and check the output directory for results, plots, and the database file.

---

## 2B. Option 2: Extending the Existing Parser Class with New Functions

Instead of making a new class, you can simply add new functions to the existing `InstrumentDataParser` class. This keeps all parsing logic in one place and is often easier for beginners.

### Step 1: Open the Existing Parser
- Open `idp/instrument_data_parser_oo.py`.

### Step 2: Add Your New Function
- Add a new method for your data type. For example:

```python
def parse_my_new_data(self):
    # Your code to read/process your new data type
    return metadata_list, failed_files
```

### Step 3: Use Your New Function in Your Script
- In your test or main script, call your new function:

```python
parser = InstrumentDataParser(folder_locations, sqlite_file_path)
my_metadata, failed_files = parser.parse_my_new_data()
```

- Then pass the results to the outputer as before:

```python
outputer.save_to_db(my_df, dataset_type='mydata')
outputer.create_summary_plots(my_df, 'mydata', dataset_type='mydata')
```

---

## 2C. Which Approach Should I Use?

- **New Class:**
  - Best if your data is very different or you want to keep things separate.
  - Good for advanced users or large projects.
- **Add Functions:**
  - Easiest for beginners.
  - Keeps all parsing in one file/class.
  - Great for incremental additions.

---

## 3. Visual Diagram

A D2 diagram visualizing the workflow is shown below and in `documentation/Expanding_to_New_Domains.svg`. The diagram applies to both approaches: whether you make a new class or just add new functions, the flow is the same.
![Instrument_Data_Parser_Workflow](https://github.com/user-attachments/assets/b19ce406-c827-481e-8141-3ee2cd598a8f)


---

## 4. Troubleshooting & Best Practices
- Start simple: get your parser to print out a few rows of data before adding database or plotting features.
- Use print statements to debug.
- If you get database errors, check that your DataFrame columns match your table schema.
- Use the retry logic in the outputer for robust saving.
- Ask for help or check the README if you get stuck!

---

Happy parsing! 
