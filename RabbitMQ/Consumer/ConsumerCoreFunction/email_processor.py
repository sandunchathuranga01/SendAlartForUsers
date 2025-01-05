from RabbitMQ.Consumer.ConsumerCoreFunction.data_fetcher import get_case_data, get_drc_data
from RabbitMQ.Consumer.ConsumerCoreFunction.email_sender import  send_email
import os


def load_template(template_id):
    try:
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the absolute path to the template
        template_path = os.path.join(script_dir, "../templates", f"{template_id}.html")

        with open(template_path, "r") as template_file:
            return template_file.read()
    except FileNotFoundError:
        print(f"Template with ID {template_id} not found.")
        return None


def process_email(case_id, template_id):
    # Fetch data using CaseID
    case_data = get_case_data(case_id)
    if not case_data:
        print("No case data found. Aborting email process.")
        return

    # Extract DRC data from the `drc` array
    drc_array = case_data.get("drc")
    if not drc_array or len(drc_array) == 0:
        print("DRC data not found in case data. Aborting email process.")
        return

    # Assuming we take the first DRC object in the array
    drc_data_in_case = drc_array[0]
    drc_id = drc_data_in_case.get("drc_id")

    # Fetch DRC details using DRC_ID from the drcDetails collection
    drc_data = get_drc_data(drc_id)
    if not drc_data:
        print("No DRC data found for DRC_ID. Aborting email process.")
        return

    drc_name = drc_data.get("DRC_Name")
    email = drc_data.get("Email")


    if not drc_name or not email:
        print("DRC_Name or Email not found in DRC data. Aborting email process.")
        return

    # Load the selected email template
    template = load_template(template_id)
    if not template:
        print("Failed to load template. Aborting email process.")
        return

    # Replace placeholders in the template
    email_content = template.replace("{{CaseID}}", case_id)
    email_content = email_content.replace("{{Email}}", email)
    email_content = email_content.replace("{{CreatedBy}}", drc_name)

    # Send the email
    subject = "Case Alert Notification"
    send_email(email, subject, email_content)


if __name__ == "__main__":
    process_email("CASE001", "TEMP01")


