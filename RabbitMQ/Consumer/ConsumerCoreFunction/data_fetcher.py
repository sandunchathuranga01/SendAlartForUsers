from pymongo import MongoClient
from Config.db_Config import get_db_details

def get_case_data(case_id):
    try:
        # Get database details
        db_details = get_db_details()
        mongo_url=db_details["mongo_url"]
        database_name = db_details["database_name"]
        case_collection_name = db_details["case_collection_name"]

        # Connect to MongoDB
        client = MongoClient(mongo_url)
        db = client[database_name]
        collection = db[case_collection_name]

        # Fetch data by CaseID
        case_data = collection.find_one({"case_id": case_id})
        if case_data:
            return case_data
        else:
            print(f"No data found for CaseID: {case_id}")
            return None
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_drc_data(drc_id):
    try:
        # Get database details
        db_details = get_db_details()
        mongo_url=db_details["mongo_url"]
        database_name = db_details.get("database_name")
        drc_collection_name = db_details.get("drc_collection_name")

        # Connect to MongoDB
        client = MongoClient(mongo_url)
        db = client[database_name]
        collection = db[drc_collection_name]

        # Fetch data by DRC_ID (case-sensitive)
        drc_data = collection.find_one({"DRC_ID": drc_id})
        if drc_data:
            return drc_data
        else:
            print(f"No data found for DRC_ID: {drc_id}")
            return None
    except Exception as e:
        print(f"Error connecting to MongoDB or fetching data: {e}")
        return None
