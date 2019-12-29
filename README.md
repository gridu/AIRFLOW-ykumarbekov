## GD Airflow course project

_project structure:_
- root folder  
-- config: contains different configuration files  
   as example: variables.json requires for variables populating  
-- dags: main dags folder, contains sub-folders (prod, test)  
  add_dag_bags.py - uses for adding sub-folders to the main DAG folder  
-- docker-compose: contains yml files for deploying docker images  
   (for our project we use docker-compose-localexecutor.yml)  
-- plugins: contains custom plugins  

_Steps for deploying project:_  
1) Clone project from github
2) Edit docker-compose-localexecutor.yml - changes Volumes for actual folders  
3) Start docker-compose with command:    
```
docker-compose -f ./docker-compose-localexecutor.yml up -d
```
4) Upload variables from config folder with command:  
```
docker exec docker-compose_webserver_1 bash -l -c "airflow variables -i /usr/local/airflow/config/variables.json"
```
5) Check everything is working and open web page: `http://localhost:8080/admin` all dags must be opened on the main page  
6) Edit connection information for the Postgres database: `use UI -> Admin -> connection -> postgres_default`  
6.1) You have to add additional folder inside container:  
```
docker exec docker-compose_webserver_1 bash -l -c "mkdir /tmp/airflow_project"
```
7) Run the DAGs  

_Short description:_  
All functionality is implemented in the prod folder, where we have 3 Python files:  
- trigger_dag.py: main DAG which start the workflow, first task is waiting - run file (/tmp/airflow_project/run)  
- db_job_dag.py: all operations connected with database  
- subdag.py: invoking subDAG which waits until db_jb_dog.py is finished then runs cleaning process  




   
