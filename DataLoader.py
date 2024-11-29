import json

class DataReader:
    def __init__(self, data_path):
        self.data_path = data_path

    def load_maintenance_logs(self):
        with open(f"{self.data_path}/maintenance_logs1.json") as f:
            return json.load(f)
    
    def load_vehicle_data(self):
        with open(f"{self.data_path}/vehicle_info1.json") as f:
            return json.load(f)
    
    def load_route_data(self):
        with open(f"{self.data_path}/route_info.json") as f:
            return json.load(f)
    
    def load_fleet_data(self):
        with open(f"{self.data_path}/fleet_info.json") as f:
            return json.load(f)
    
    def load_warehouse_data(self):
        with open(f"{self.data_path}/warehouse_info.json") as f:
            return json.load(f)
    
    def load_order_data(self):
        with open(f"{self.data_path}/order_info1.json") as f:
            return json.load(f)