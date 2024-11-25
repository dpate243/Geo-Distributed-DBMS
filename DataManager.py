import datetime
import json
import random


class DataManage:
    def __init__(self, db_clients):
        self.db_clients = db_clients
    
    def get_shard_regions(self, record):
        '''
        gets all affected regions for a particular query. (Always contains maindb)
        '''
        shard_regions=[]

        all_regions = [r.lower() for r in list(self.db_clients.keys())]
        all_regions.remove('main')

        region_attr = record.get('region', "").lower()
        source_region_attr = record.get('source_region', "").lower()
        destination_region_attr = record.get('destination_region', "").lower()

        if region_attr in self.db_clients:
            shard_regions.append(region_attr)
        if source_region_attr in self.db_clients:
            shard_regions.append(source_region_attr)
        if destination_region_attr in self.db_clients:
            shard_regions.append(destination_region_attr)
        
        # full replication
        if len(shard_regions)==0:
            return self.db_clients.keys()
        
        if source_region_attr and destination_region_attr and source_region_attr == destination_region_attr:
            all_regions.remove(source_region_attr)
            shard_regions.append(random.choice(all_regions))
        
        shard_regions.insert(0, 'main')
        shard_regions = list(set(shard_regions))
        return shard_regions
    

    def insert_data(self, collection_name, record):
        shard_regions = self.get_shard_regions(record)

        for region in shard_regions:
            self.db_clients[region][collection_name].insert_one(record)


    def update_data(self, collection_name, query, update_value):
        '''
        Queries maindb for data, finds affected regions and updates data in those affecting regions
        '''

        #TODO replace find with retrieve_data
        data = self.db_clients["main"]["SupplyOne"][collection_name].find(query)

        shard_regions = self.get_shard_regions(data)

        for region in shard_regions:
            self.db_clients[region][collection_name].update_one(query, {"$set": update_value})


    def delete_data(self, collection_name, query):
        '''
        Queries maindb for data, finds affected regions and deletes data in those affecting regions
        '''

        #TODO: replace find with retrieve_data
        data = self.db_clients["main"]["SupplyOne"][collection_name].find(query)

        shard_regions = self.get_shard_regions(data)

        for region in shard_regions:
            self.db_clients[region][collection_name].delete_one(query)


    def insert_records(self, collection_name, records):
        for record in records:
            self.insert_data(collection_name, record)
