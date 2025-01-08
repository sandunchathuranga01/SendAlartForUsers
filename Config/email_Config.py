#Returns the MongoDB connection details
def get_email_details():
    email_details = {
        "smtp_server": "smtp.gmail.com", # SMTP server name
        "smtp_port": 587, #SMTP port
        "sender_email": "chathuranga.development@gmail.com", # SMTP sending email
        "sender_password": "fdgm wdbl mekk vuvl" # app password
    }
    return email_details