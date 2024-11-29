# Geo-Distributed-DBMS

## 1. Installation and Setup

1. Open docker desktop (for windows and mac)

2. Make sure you get the *.env* file and paste it in the project directory root.

3. Open command promt in the **project directory root**

4. shell 1:-

        docker compose up

    It will take up to 5 mins to compose up docker containers (do not close the shell)

5. shell 2:-
    
        docker build -t supplyone .
        docker run -it --rm supplyone

6. Once you enter the container in shell 2 run the main.py file
    
        python main.py

    *optional args:
    
    --path : path to the json containing query in the format {'query':query, 'collection_name': collection}

## 2. Trigger Airflow DAG

- Login Airflow at http://localhost:8080/

    ![alt text](images/login.png)

    Username: airflow

    Password: airflow

- Go to DAGs

    ![alt text](images/dags.png)

- Click on "Trigger DAG" button located on the top-right corner for "process_orders_dag"

    ![alt text](images/trigger.png)


## 3. Run Queries
