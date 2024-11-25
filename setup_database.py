import os
from dotenv import load_dotenv

load_dotenv()


def setup_database(db, region):
    """
    Sets up collections in the MongoDB database with appropriate validation rules.

    Args:
        db: A pymongo.database.Database object.
    """
    db.drop_database("SupplyOne")
    db = db["SupplyOne"]

    collection_validators = {
        os.getenv("ORDERS_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["order_id", "order_size", "source_warehouse_id", "destination_warehouse_id", 
                           "source_region", "destination_region", "order_date", "order_status"],
                "properties": {
                    "order_id": {"bsonType": "string", "description": "Unique ID for the order"},
                    "order_size": {"bsonType": "int", "description": "Size of the order"},
                    "source_warehouse_id": {"bsonType": "string", "description": "ID of the source warehouse"},
                    "destination_warehouse_id": {"bsonType": "string", "description": "ID of the destination warehouse"},
                    "source_region": {"bsonType": "string", "description": "Region of source warehouse"},
                    "destination_region": {"bsonType": "string", "description": "Region of destination warehouse"},
                    "order_date": {"bsonType": "string", "description": "Date of the order (YYYY-MM-DD)"},
                    "order_status": {"bsonType": "string", "enum": ["processed", "unprocessed"], "description": "Order status"}
                }
            }
        },

        os.getenv("WAREHOUSE_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["warehouse_id", "city", "location", "region"],
                "properties": {
                    "warehouse_id": {"bsonType": "string", "description": "Unique warehouse ID"},
                    "city": {"bsonType": "string", "description": "Warehouse city"},
                    "location": {
                        "bsonType": "array",
                        "description": "Geospatial location data"
                    },
                    "region": {"bsonType": "string", "description": "Region of the warehouse"}
                }
            }
        },

        os.getenv("FLEET_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["fleet_id", "source_warehouse_id", "destination_warehouse_id", "source_region", 
                           "destination_region", "orders", "truck_id", "space_available"],
                "properties": {
                    "fleet_id": {"bsonType": "string", "description": "Unique fleet ID"},
                    "source_warehouse_id": {"bsonType": "string", "description": "Source warehouse ID"},
                    "destination_warehouse_id": {"bsonType": "string", "description": "Destination warehouse ID"},
                    "source_region": {"bsonType": "string", "description": "Source region"},
                    "destination_region": {"bsonType": "string", "description": "Destination region"},
                    "orders": {"bsonType": "array", "description": "List of order IDs"},
                    "truck_id": {"bsonType": "string", "description": "Associated truck ID"},
                    "space_available": {"bsonType": "int", "description": "Available capacity"}
                }
            }
        },

        os.getenv("VEHICLE_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["truck_id", "make", "model", "capacity"],
                "properties": {
                    "truck_id": {"bsonType": "string", "description": "Unique truck ID"},
                    "make": {"bsonType": "string", "description": "Vehicle make"},
                    "model": {"bsonType": "string", "description": "Vehicle model"},
                    "capacity": {"bsonType": "int", "description": "Capacity of the vehicle"}
                }
            }
        },

        os.getenv("ROUTE_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["route_id", "source_wh", "destination_wh", "source_region", 
                           "destination_region", "travel_time"],
                "properties": {
                    "route_id": {"bsonType": "string", "description": "Unique route ID"},
                    "source_wh": {"bsonType": "string", "description": "Source warehouse ID"},
                    "destination_wh": {"bsonType": "string", "description": "Destination warehouse ID"},
                    "source_region": {"bsonType": "string", "description": "Source region"},
                    "destination_region": {"bsonType": "string", "description": "Destination region"},
                    "travel_time": {"bsonType": "int", "description": "Travel time in minutes"}
                }
            }
        },

        os.getenv("MAINTENANCE_COLLECTION"): {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["log_id", "truck_id", "service_date", "service_type", "city", "details"],
                "properties": {
                    "log_id": {"bsonType": "string", "description": "Unique maintenance log ID"},
                    "truck_id": {"bsonType": "string", "description": "Truck ID"},
                    "service_date": {"bsonType": "string", "description": "Service date (YYYY-MM-DD)"},
                    "service_type": {"bsonType": "string", "description": "Type of service"},
                    "city": {"bsonType": "string", "description": "City of the service"},
                    "details": {"bsonType": "string", "description": "Details about the service"}
                }
            }
        }
    }

    for collection_name, validator in collection_validators.items():
        try:
            db.create_collection(collection_name)
        except Exception as e:
            print(f"Error handling collection {collection_name}: {str(e)}")
        
        try:
            db.command("collMod", collection_name, validator=validator)
        except Exception as e:
            print(f"Error applying validator for {collection_name}: {str(e)}")

    print(f"Database setup complete for {region} region.")