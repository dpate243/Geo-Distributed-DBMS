from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from connect import connect_to_databases
from DataManager import DataManage
from QueryProcessor import GeoDistributedQueryProcessor
import random 

# MongoDB insert function
def processing():
    # Set up MongoDB connection
    databases =  connect_to_databases()
    db_clients = {}

    for region in databases:
        db_clients[region] = databases[region]["SupplyOne"]

    manage = DataManage(db_clients)
    GDQP = GeoDistributedQueryProcessor(db_clients)

    known_warehouse_ids = ['LAX-WH', 'PHX-WH', 'NYC-WH', 'ATL-WH', 'DAL-WH', 'CHI-WH']
    source_wh, dest_wh = random.sample(known_warehouse_ids, 2)
    #source_wh, dest_wh = 'LAX-WH', 'PHX-WH' 


    fleetQ = [
        {
            '$match': {
                'source_warehouse_id': source_wh,
                'destination_warehouse_id': dest_wh
            }
        }
    ]

    fleet = GDQP.execute_query_nearest(fleetQ, 'FleetInfo')

    if len(fleet)==0: 
        print(f'No fleet found for the {source_wh}, {dest_wh} pair')
        return
    else:
        fleet=fleet[0]
        print(fleet)
    
    ordersQ = [
        {
            '$match': {
                'order_status': 'unprocessed',
                'order_size': {'$lte': fleet['space_available']},
                'source_warehouse_id': source_wh,
                'destination_warehouse_id': dest_wh
            }
        },
        {
            '$sort': {'order_date': 1}  
        }
    ]

    eligible_orders = GDQP.execute_query_nearest(ordersQ, 'Orders')

    if not eligible_orders: 
        print(f'No unprocessed orders found for {source_wh}, {dest_wh} pair')
        return
    else: 
        order = eligible_orders[0]
        print(order)
    
    query1 = [
        {
            '$match': {
                'order_id': order['order_id']
            }
        }
    ]

    update_value1 = {'$set': {'order_status': 'processed'}}

    results1 = manage.update_data('Orders', query1, update_value1)
    print(results1)

    query2 =  [
        {
            '$match': {
                'fleet_id': fleet['fleet_id']
            }
        }
    ]

    update_value2 = {
        '$push': {'orders': order['order_id']},
        '$inc': {'space_available': -order['order_size']}
    }
    
    results2 = manage.update_data('FleetInfo', query2, update_value2)
    
    print('Sucssfull Processing!')
     
    

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'process_orders_dag',
    default_args=default_args,
    description='A simple DAG to insert data into MongoDB',
    schedule_interval='* * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # PythonOperator to call the insert_mongo function
    update_data_task = PythonOperator(
        task_id='update_data_task',
        python_callable=processing
    )

# Task order
update_data_task
