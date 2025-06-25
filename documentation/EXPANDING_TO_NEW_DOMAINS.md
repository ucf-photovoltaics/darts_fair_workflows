# Expanding the Instrument Data Parser Workflow to New Domains

Welcome! This guide will help you adapt the instrument data parser workflow for your own research data, even if you have little or no experience with Python or databases. The workflow is modular and designed to be easy to extend.

---

## 1. Workflow Overview

The workflow is made up of four main parts:
- **Parser**: Reads and processes your raw data files.
- **Outputer**: Handles saving results, making plots, and writing to the database.
- **SQLiteDB**: Manages all database operations.
- **Plotter**: (Optional) Makes visualizations from your data.

See the diagram in `documentation/Expanding_to_New_Domains.d2` for a visual overview.

---

## 2. Step-by-Step: Creating Your Own Data Parser

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
- Use or subclass `InstrumentDataParserOutputer` (see `idp/instrument_data_parser_outputer.py`).
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

## 3. Visual Diagram

A D2 diagram showing the workflow is in `documentation/Expanding_to_New_Domains.d2`. You can view it as a SVG or edit it with any D2-compatible tool.

---

## 4. Troubleshooting & Best Practices
- Start simple: get your parser to print out a few rows of data before adding database or plotting features.
- Use print statements to debug.
- If you get database errors, check that your DataFrame columns match your table schema.
- Use the retry logic in the outputer for robust saving.
- Ask for help or check the README if you get stuck!

---

## 5. Additional Resources
- See the original `test_instrument_parser.py` for a full example.
- The `README.md` has more details on setup and requirements.
- The D2 diagram is a great way to explain your workflow to collaborators.

---

Happy parsing! 