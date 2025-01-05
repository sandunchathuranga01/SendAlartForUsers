#this function use to store config details of database(mongodb)
def get_db_details():
    db_details = {
        "mongo_url":"mongodb://localhost:27017/", #mongodb URL
        "database_name": "DRS", #mongodb Database
        "case_collection_name": "caseDetails", #case collection name
        "drc_collection_name": "drcDetails", #drc collection name
        "status_config_collection_name": "statusConfig" #status config collection name
    }
    return db_details #return MongoDB config details
