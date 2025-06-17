Airflow_Env, when run, will create an Apache Airflow environment within a Docker container.

To deploy this to your local machine, make sure you have Docker installed and configured to run. You can learn more about Docker here: https://docs.docker.com/get-started/

To create the container, open a command terminal, navigate to the file location “airflow_env”. Run to command “docker-compose up -d” 
To stop the containers, run the command “docker-compose down” or, using Docker Desktop, you can stop the containers via the GUI.

To access the Airflow dashboard, open a web browser can go to “localhost:8085”. The port number is configurable via the docker-compose.yaml file. 

To log in to the Airflow dashboard, user: airflow, password: airflow. This is also configurable via the docker-compose.yaml file.  

Once started, you should see one DAG populated: “Update_Database_MFR_IR.” The DAG is paused by default; once started, the DAG can be manually triggered when necessary. The DAG is scheduled to run once a day, every day at midnight UTC -4.
