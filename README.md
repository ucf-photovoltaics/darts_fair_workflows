# darts_fair_workflows
https://ucf-dpv.notion.site/FAIR-Computational-Workflow-Implementation-in-Photovoltaic-Research-1e88d52e715180e985c4dc18c9c215c0?pvs=4
## Airflow v1 Design for PV Data Processing

This document outlines a v1 Airflow design for orchestrating the execution of Python scripts that process photovoltaic (PV) data at the Florida Solar Energy Center (FSEC). The design leverages existing scripts to create a functional workflow for managing and updating the PVMCF database.

### 1.  Conceptual DAG Structure

The core of the design is a primary Airflow DAG that manages the execution of individual data processing pipelines. Each pipeline corresponds to a specific Python script. The DAG is structured as follows:

* **Pipeline Tasks:** These tasks execute the data processing scripts.
    * `run_darkiv_pipeline`: Executes `darkiv_pipeline.py` to process Dark IV measurement data.
    * `run_el_pipeline`: Executes `el_pipeline.py` to process Electroluminescence (EL) image data.
    * `run_ir_indoor_pipeline`: Executes `ir_indoor_pipeline.py` to process Indoor Infrared (IR) image data.
    * `run_mfr_pipeline`: Executes `mfr_pipeline.py` to process Sinton IV measurement data.
    * `run_uvf_indoor_pipeline`: Executes `uvf_indoor_pipeline.py` to process Indoor Ultraviolet Fluorescence (UVF) image data.
* **Database Update Task:** This task updates the central PVMCF database after the pipeline tasks have completed.
    * `update_pvmcf_database`: Executes `update_database.py` to integrate the processed data into the PVMCF database.

### 2.  Task Dependencies

The workflow enforces a clear dependency: the database update task (`update_pvmcf_database`) is triggered *only after* all pipeline tasks have finished successfully. This ensures that the database is updated with the latest processed data.

This dependency is represented in Airflow as:
[run_darkiv_pipeline, run_el_pipeline, run_ir_indoor_pipeline, run_mfr_pipeline, run_uvf_indoor_pipeline] >> update_pvmcf_database
### 3.  Airflow Operators

In this v1 design, we can use Airflow operators to execute the Python scripts. Two primary options are available:

* **`BashOperator`**:
    * Executes shell commands.
    * Simpler for initial implementation.
    * Example: `python /path/to/your/darkiv_pipeline.py`
* **`PythonOperator`**:
    * Executes Python callables (functions).
    * More Airflow-native and integrated.
    * Requires refactoring script logic into functions.

**Recommendation (for v1):** `BashOperator` offers a quicker way to integrate the existing scripts with minimal modification. However, `PythonOperator` is generally preferred for long-term maintainability and Airflow best practices.

### 4.  v1 Design Considerations

* **Error Handling:**
    * Initially, rely on the `try-except` blocks and logging within the Python scripts.  The scripts include error handling for file processing and database operations.
    * Incorporate Airflow's retry mechanisms and failure notifications in future versions.
* **Configuration Management:**
    * Use Airflow variables or environment variables to manage file paths, database connection strings, and other configuration parameters.  The scripts use hardcoded file paths that should be externalized.
* **Logging:**
    * The `update_database.py` script includes logging. Integrate with Airflow's logging for centralized monitoring.
* **Parallelism:**
    * Airflow can potentially execute the pipeline tasks in parallel, improving efficiency.  Ensure sufficient resources are available.

### 5.  Implementation Steps

1.  **Operator Selection:** Choose between `BashOperator` and `PythonOperator`.
2.  **DAG File Creation:** Write the Airflow DAG definition in a Python file.
    * Import necessary operators.
    * Define the `DAG` object.
    * Create `Task` objects for each pipeline and the database update.
    * Specify the execution command (for `BashOperator`) or Python callable (for `PythonOperator`).
    * Define task dependencies.
3.  **Path Configuration:** Ensure that file paths, such as those for the data files and the database, are correctly configured within the Airflow tasks.  Use Airflow variables or environment variables.
4.  **Local Testing:** Thoroughly test the DAG locally to verify correct script execution and data flow.

### 6.  Future Enhancements

This v1 design provides a starting point. Future improvements may include:

* **Refactoring:** Converting scripts to Python callables for `PythonOperator`.
* **Advanced Airflow Features:** Using Branching, SubDAGs, or TaskGroups for more complex workflows.
* **Data Quality Checks:** Adding tasks to validate data integrity.
* **Alerting:** Implementing robust alerting on task failures.
* **Modularization:** Further break down the pipelines into smaller, reusable Airflow tasks.
* **Dynamic DAG Generation:** Creating DAGs dynamically based on configuration stored in a database or external file.
