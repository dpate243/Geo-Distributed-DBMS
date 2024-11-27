from connect import connect_to_databases
from DataManager import DataManage
from QueryProcessor import GeoDistributedQueryProcessor

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


results = GDQP.execute_query_nearest(pipeline, 'Orders')

for result in results:
    print(result)