import time
from dotenv import load_dotenv
from bson import json_util

load_dotenv()


class GeoDistributedQueryProcessor:
    def __init__(self, clusters):
        self.clusters = clusters
    
    def process_query(self, query, collection_name, primary, secondary):
        primary_cluster = self.clusters[primary]
        secondary_cluster = self.clusters[secondary]

        primary_collection = primary_cluster[collection_name]
        secondary_collection = secondary_cluster[collection_name]

        primary_results = list(primary_collection.find())
        secondary_results = list(secondary_collection.find())
        
        combined_docs = []
        seen_ids = set()
        
        for doc in primary_results + secondary_results:
            doc_id = doc.get('_id')
            if doc_id not in seen_ids:
                combined_docs.append(doc)
                seen_ids.add(doc_id)
        
        temp_collection_name = f"temp_{collection_name}{primary}{secondary}"
        temp_db = primary_cluster
        temp_collection = temp_db[temp_collection_name]
        
        try:
            if combined_docs:
                temp_collection.insert_many(combined_docs)
            
            combined_results = list(temp_collection.aggregate(query))
            
            return combined_results
        
        finally:
            temp_collection.drop()


    def get_cluster_preference(self):
        cluster_preference = {}
        search_clusters = self.clusters.copy()
        search_clusters.pop('main')

        for region, cluster in search_clusters.items():
            start_time = time.time()
            cluster.command('ping')
            end_time = time.time()
            cluster_preference[region] = (end_time - start_time) * 1000

        cluster_preference = dict(sorted(cluster_preference.items(), key=lambda item: item[1]))
        return cluster_preference

    def execute_query_nearest(self, query, collection_name):
        cluster_preference = self.get_cluster_preference()
        
        primary = list(cluster_preference.keys())[0]
        secondary = list(cluster_preference.keys())[1]

        return self.process_query(query, collection_name, primary, secondary)




    def execute_query(self, query, collection_name, primary=None, secondary=None, nearest=False):
        if not (primary and secondary) and not nearest:
            return "Error: Please provide either primary, secondary or nearest preference."

        if nearest:
            return self.execute_query_nearest(query, collection_name)
        else:
            return self.process_query(query, collection_name, primary, secondary)
