from pymongo import MongoClient
from datetime import datetime
from Config.db_Config import get_db_details
from RabbitMQ.Producer.ProducerCoreFunction.Email_Producer import send_message_rq

# MongoDB connection details
db_details = get_db_details()
mongo_url = db_details["mongo_url"]
database_name = db_details["database_name"]
case_collection_name = db_details["case_collection_name"]
status_config_collection_name = db_details["status_config_collection_name"]

# Function to calculate date count for expired date
def calculate_date_count(expired_dtm):
    if not expired_dtm:
        return None
    try:
        if isinstance(expired_dtm, datetime):
            expired_date = expired_dtm
        else:
            expired_date = datetime.strptime(expired_dtm, "%Y-%m-%dT%H:%M:%S")
        current_date = datetime.now()
        date_count = (expired_date - current_date).days
        return date_count
    except ValueError:
        print(f"Invalid date format: {expired_dtm}")
        return None

# Function to retrieve "before_days", "Status_Type", and "Temp_ID" from statusConfig collection
def get_status_config_details(status_type_id):
    client = MongoClient(mongo_url)
    db = client[database_name]
    status_config_collection = db[status_config_collection_name]

    status_config = status_config_collection.find_one({"Status_Type_ID": status_type_id})
    if status_config:
        return {
            "before_days": status_config.get("Before_Days", None),
            "status_type": status_config.get("Status_Type", None),
            "temp_id": status_config.get("Temp_ID", None)
        }
    return {"before_days": None, "status_type": None, "temp_id": None}

# Function to update the notified_dtm field
def update_notified_dtm(case_id, status_type_id):
    client = MongoClient(mongo_url)
    db = client[database_name]
    case_collection = db[case_collection_name]

    current_dtm = datetime.now()
    result = case_collection.update_one(
        {"case_id": case_id, "case_status.Status_Type_ID": status_type_id},
        {"$set": {"case_status.$.notified_dtm": current_dtm}}
    )

    if result.modified_count > 0:
        print(f"Updated notified_dtm for case_id: {case_id}, status_type_id: {status_type_id}")
    else:
        print(f"No document updated for case_id: {case_id}, status_type_id: {status_type_id}")

# Function to retrieve all case statuses
def send_status_alerts():
    # Establish connection to MongoDB
    client = MongoClient(mongo_url)
    db = client[database_name]
    case_collection = db[case_collection_name]

    # Query the collection for all documents
    cases = case_collection.find()

    # Extract and process the status data from each case
    for case in cases:
        case_statuses = case.get("case_status", [])
        for status in case_statuses:
            if status.get("notified_dtm"):
                continue

            # Assign variables for clarity
            case_id = case.get("case_id")
            status_type_id = status.get("Status_Type_ID")
            notified_dtm = status.get("notified_dtm")
            expired_dtm = status.get("expired_dtm")
            date_count = calculate_date_count(expired_dtm)

            status_config_details = get_status_config_details(status_type_id)
            before_days = status_config_details["before_days"]
            status_type = status_config_details["status_type"]
            temp_id = status_config_details["temp_id"]

            exchange = 'alert_exchange[DE]' #email send exchange
            routing_key = 'Email' #email consumer routing key

            #send data to email producer
            send_message_rq(exchange, routing_key, case_id, temp_id)

            # Update notified_dtm after sending the message
            update_notified_dtm(case_id, status_type_id)

            # Print the collected variables
            print({
                "case_id": case_id,
                "notified_dtm": notified_dtm,
                "date_count": date_count,
                "before_days": before_days,
                "status_type": status_type,
                "temp_id": temp_id
            })

# Main function to run the sendAlartToDrc()
def sendAlartToDrc():
    send_status_alerts()
    print("All status data has been retrieved and processed.")

if __name__ == "__main__":
    sendAlartToDrc()