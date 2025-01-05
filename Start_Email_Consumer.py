import pika
import json

from RabbitMQ.Consumer.ConsumerCoreFunction.email_processor import process_email


def receive_messages():
     exchange_name = 'alert_exchange[DE]'
     routing_key = 'Email'
     queue_name = 'send_email'

     # Connect to RabbitMQ server
     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
     channel = connection.channel()

     # Declare an exchange
     channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

     # Declare a queue
     channel.queue_declare(queue=queue_name)

     # Bind the queue to the exchange with the routing key
     channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

     print(f" [*] Waiting for messages in queue '{queue_name}'. To exit press CTRL+C")

     # Callback function to process messages
     def callback(ch, method, properties, body):
         try:
             # Parse the JSON message
             message = json.loads(body)
             case_id = message.get("Case_ID")
             template_id = message.get("Temp_ID")

             # Process the extracted variables
             if case_id and template_id:
                 print(f" [x] Received Case_ID: {case_id}, Temp_ID: {template_id}")
                 process_email(case_id, template_id)
             else:
                 print(" [!] Invalid message format. Case_ID or Temp_ID missing.")
         except json.JSONDecodeError:
             print(" [!] Error decoding message body as JSON")

     # channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
     # channel.start_consuming()

     # Start consuming messages
     try:
         channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
         channel.start_consuming()

     except KeyboardInterrupt:
         print("Interrupted by user. Shutting down...")

if __name__ == "__main__":
     receive_messages()

