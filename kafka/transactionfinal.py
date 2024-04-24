import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

Order_KAFKA_TOPIC ="order_details"
ORDER_CONFIRMED_KAFKA_TOPIC= "order_confirmed"

consumer = KafkaConsumer(
    Order_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
              api_version=(0,11,5))

print("Starting to  listen")

while True:

    for message in consumer:
        print("Ongoing transaction")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        user_id = consumed_message["user-id"]
        total_cost = consumed_message["total-cost"]

        data = {
            "customer-id": user_id,
            "customer-email": f"{user_id}@gmail.com",
            "total-cost": total_cost
        }

        print("success")

        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )