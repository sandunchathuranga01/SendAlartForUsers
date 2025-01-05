import pika
import json

def send_message(exchange_name, routing_key, case_id, temp_id):
    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare an exchange
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    # Create a JSON message
    message = json.dumps({"Case_ID": case_id, "Temp_ID": temp_id})

    # Publish the message
    channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)
    print(f" [x] Sent '{message}' to exchange '{exchange_name}' with routing key '{routing_key}'")

    # Close the connection
    connection.close()

if __name__ == "__main__":
    exchange = 'alert_exchange[DE]'
    routing_key = 'Email'
    case_id = "CASE001"
    temp_id = "TEMP01"
    send_message(exchange, routing_key, case_id, temp_id)
