from pymongo import MongoClient
from datetime import datetime

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017"  # Replace with your MongoDB URI
DATABASE_NAME = "DRC"  # Replace with your database name
COLLECTION_NAME = "caseDetails"  # Replace with your collection name


def format_created_dtm():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Check if the collection is empty
    if collection.count_documents({}) == 0:
        print("The collection is empty.")
        return

    # Debug: Print all documents in the collection
    print("Fetching all documents for debugging:")
    for doc in collection.find():
        print(doc)

    # Fetch the document with specific criteria
    document = collection.find_one({"case_id": "CASE001"})  # Replace filter with your criteria

    if document:
        print("Document found:", document)
        if "created_dtm" in document:
            created_dtm = document["created_dtm"]
            print(f"Original created_dtm from MongoDB: {created_dtm}")

            try:
                # Parse the datetime (assuming ISO 8601 format)
                dt_object = datetime.fromisoformat(str(created_dtm).replace("Z", "+00:00"))

                # Print in multiple formats
                print("Formatted Date-Time:")
                print("YYYY-MM-DD HH:MM:SS:", dt_object.strftime("%Y-%m-%d %H:%M:%S"))
                print("MM/DD/YYYY:", dt_object.strftime("%m/%d/%Y"))
                print("Day, Month Date, Year:", dt_object.strftime("%A, %B %d, %Y"))
                print("ISO 8601 format:", dt_object.isoformat())
                print("Custom format (e.g., DD-MM-YYYY):", dt_object.strftime("%d-%m-%Y"))
            except ValueError as e:
                print(f"Error parsing 'created_dtm': {e}")
        else:
            print("The document does not contain the 'created_dtm' field.")
    else:
        print("No document found with the specified criteria.")


if __name__ == "__main__":
    format_created_dtm()
