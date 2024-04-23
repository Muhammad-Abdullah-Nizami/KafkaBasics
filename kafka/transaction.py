import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

Order_KAFKA_TOPIC ="order_details"


consumer = KafkaConsumer(
    Order_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

print("Starting to  listen")

while True:

    for message in consumer:
        print("Ongoing transaction")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

