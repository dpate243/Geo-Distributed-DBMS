import os
import time
import ast
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

        primary_results = list(primary_collection.aggregate(query))
        secondary_results = list(secondary_collection.aggregate(query))

        combined_results = primary_results + secondary_results

        json_strings = [json_util.dumps(doc) for doc in combined_results]
        unique_json_strings = list(set(json_strings))

        unique_results = [json_util.loads(item) for item in unique_json_strings]

        return unique_results


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
