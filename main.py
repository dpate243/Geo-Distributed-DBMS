from connect import connect_to_databases
from DataManager import DataManage
from QueryProcessor import GeoDistributedQueryProcessor
import json
import argparse

def main(qpath=None):

    databases =  connect_to_databases()
    db_clients = {}

    for region in databases:
        db_clients[region] = databases[region]["SupplyOne"]

    manage = DataManage(db_clients)
    GDQP = GeoDistributedQueryProcessor(db_clients)
    
    pipeline = [
        {
            "$group": {
                "_id": "$source_warehouse_id",
                "total_order_size": { "$sum": "$order_size" }
            }
        },
        { "$sort": { "total_order_size": -1 } },
        { "$limit": 3 }
    ]
    
    collection = 'Orders'        

    if qpath!=None:
        with open(qpath, 'r') as f: querydata = json.load(f)
        collection = querydata['collection_name']
        pipeline = querydata['query']


    results = GDQP.execute_query_nearest(pipeline, collection)

    for result in results:
        print(result)
       
       
        
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, help="Path to the query json")

    args = parser.parse_args()

    if not args.path: main()
    else: main(args.path)
    