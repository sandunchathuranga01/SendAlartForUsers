import pandas as pd
from pymongo import MongoClient
import numpy as np

# Load Excel file using pandas lib
def load_excel_data(file_path):
    excel_data = pd.ExcelFile(file_path)
    return {
        "case_details": excel_data.parse('Case_Details'),
        "drc": excel_data.parse('DRC'),
        "approve": excel_data.parse('Approve'),
        "contact": excel_data.parse('Client_Contact'),
        "status": excel_data.parse('Status')
    }

# Connect to MongoDB database
def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['DRS']
    return db['caseDetails']

# Helper function to convert data to native Python types
def convert_to_native_types(data):
    if isinstance(data, (np.integer, np.int64)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64)):
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    elif pd.isnull(data):
        return None
    return data

# Function to build the MongoDB document structure
def build_document(row, approve_data, contact_data, drc_data, status_data):
    case_id = row['Case ID']

    # Approve data
    approve_rows = approve_data[approve_data['Case ID'] == case_id]
    approve = [
        {
            "approve_process": convert_to_native_types(approve_row['Approved process']),
            "approve_by": convert_to_native_types(approve_row['Approved by']),
            "approve_on": convert_to_native_types(approve_row['Approved on']),
            "remark": convert_to_native_types(approve_row['Remark'])
        }
        for _, approve_row in approve_rows.iterrows()
    ]


    # Contact data
    contact_rows = contact_data[contact_data['Case ID'] == case_id]
    contact = [
        {
            "mob": convert_to_native_types(contact_row['Mob']),
            "email": convert_to_native_types(contact_row['Email']),
            "lan": convert_to_native_types(contact_row['Lan']),
            "address": convert_to_native_types(contact_row['Address'])
        }
        for _, contact_row in contact_rows.iterrows()
    ]

    # DRC data
    drc_rows = drc_data[drc_data['Case ID'] == case_id]
    drc = [
        {
            "drc_id": convert_to_native_types(drc_row['DRC ID']),
            "drc_name": convert_to_native_types(drc_row['DRC  Name']),
            "order_id": convert_to_native_types(drc_row['Order ID']),
            "created_dtm": convert_to_native_types(drc_row['Created dtm']),
            "status": convert_to_native_types(drc_row['status']),
            "status_dtm": convert_to_native_types(drc_row['status dtm']),
            "case_removal_remark": convert_to_native_types(drc_row['Case Removal remark']),
            "removed_by": convert_to_native_types(drc_row['Removed by']),
            "removed_dtm": convert_to_native_types(drc_row['Removed dtm']),
            "case_transfer_dtm": convert_to_native_types(drc_row['Case transfer dtm']),
            "transferred_by": convert_to_native_types(drc_row['Transfered by'])
        }
        for _, drc_row in drc_rows.iterrows()
    ]

    # Status data
    status_rows = status_data[status_data['Case ID'] == case_id]
    case_status = [
        {
            "Status_Type_ID": convert_to_native_types(status_row['Status_Type_ID']),
            "create_dtm": convert_to_native_types(status_row['Created dtm']),
            "status_reason": convert_to_native_types(status_row['Status reason']),
            "created_by": convert_to_native_types(status_row['Created by']),
            "notified_dtm": convert_to_native_types(status_row['Notified dtm']),
            "expired_dtm": convert_to_native_types(status_row['Expire dtm'])
        }
        for _, status_row in status_rows.iterrows()
    ]

    # Build the document
    document = {
        "case_id": convert_to_native_types(row['Case ID']),
        "created_dtm": convert_to_native_types(row['Created dtm']),
        "account_no": convert_to_native_types(row['Account No.']),
        "customer_ref": convert_to_native_types(row['Customer Ref']),
        "area": convert_to_native_types(row['Area']),
        "rtom": convert_to_native_types(row['RTOM']),
        "arrears_amount": convert_to_native_types(row['Arrears Amount']),
        "action_type": convert_to_native_types(row['Action type']),
        "last_payment_dtm": convert_to_native_types(row['Last Payment Date']),
        "days_count": convert_to_native_types(row['Days Count']),
        "last_bss_reading_dtm": convert_to_native_types(row['Last BSS Reading Date']),
        "commission": convert_to_native_types(row['Commission']),
        "case_current_status": convert_to_native_types(row['Case Current Status']),
        "remark": convert_to_native_types(row['Remark']),
        "approve": approve,
        "contact": contact,
        "drc": drc,
        "case_status": case_status
    }

    return document

# Main function
def main():
    file_path = 'Case_Details.xlsx'   #excel file path
    data = load_excel_data(file_path)

    case_details_data = data['case_details']
    approve_data = data['approve']
    contact_data = data['contact']
    drc_data = data['drc']
    status_data = data['status']

    collection = get_mongo_collection()

    # Process each row in case_details_data and insert into MongoDB
    for _, row in case_details_data.iterrows():
        document = build_document(row, approve_data, contact_data, drc_data, status_data)
        collection.insert_one(document)

    print("Data migration completed.")

if __name__ == "__main__":
    main()
