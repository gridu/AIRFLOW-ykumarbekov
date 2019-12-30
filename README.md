## GD Airflow course project

_project structure:_
- root folder  
-- config: contains different configuration files, as example: variables.json requires for variables populating    
-- dags: main dags folder, contains sub-folders: prod, test  
-- docker-compose: contains some YAML files for deploying docker images  
_for our project we use docker-compose-localexecutor.yml_  
-- plugins: contains project's custom plugins  
add_dag_bags.py - uses for discovering DAG sub-folders 

_Steps for deploying project:_  
1) Clone project from github
2) Edit docker-compose-localexecutor.yml - update Volumes to actual folders  
3) Run docker-compose with command:    
```
docker-compose -f ./docker-compose-localexecutor.yml up -d
```
4) Check that docker images is started and upload variables from config folder with command:  
```
docker exec docker-compose_webserver_1 bash -l -c "airflow variables -i /usr/local/airflow/config/variables.json"
```
5) Open web page: `http://localhost:8080/admin` all dags must be opened on the main page without any errors  
6) Edit connection information for the Postgres database: `use UI -> Admin -> connection -> postgres_default`  
7) Create work folder for the project inside image docker-compose_webserver_1, as below:  
```
docker exec docker-compose_webserver_1 bash -l -c "mkdir /tmp/airflow_project"
```
7) Run main DAG: trigger_dag.py  

_Short project description:_  
_All functionality is implemented in the prod folder:_  
- trigger_dag.py - is main DAG which starts the workflow, to complete 1st task, you need to create run file inside: `/tmp/airflow_project/run` 
You can use for it docker exec command.   
- db_job_dag.py: runs all operations connected with database  
- subdag.py: invoking subDAG which waits until db_jb_dog.py is finished then runs remaining tasks  

_Course practice steps implemented in the project:_  
- Code practice: DAG creation (Part I)  
- Code practice: DAG creation (Part II)  
- The DAGBag  
- Code practice: Run the DAGs  
- Code practice: Refactor dynamic DAG creation and add Branching  
- Code practice: Add correct Trigger Rule and more tasks  
- Code practice: Create a new DAG  
- Code practice: add SubDAG and Variables in trigger DAG  
- Code Practice: install and use PostgeSQL (Part I)  
- Code practice: install and use PostgeSQL (Part II)  




   
