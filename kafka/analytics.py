from kafka import KafkaConsumer
import json
ORDER_CONFIRMED_KAFKA_TOPIC= "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers = "localhost:29092"
)

total_orders = 0
total_revenue = 0

while True:
    for message in consumer:
        print("updating analytics")
        consumed_message = json.loads(message.value.decode())

        total_cost = float(consumed_message["total-cost"])
        total_orders+=1
        total_revenue += total_cost

        print(f"Orders so far today {total_orders}")
        print(f"revenue so far today {total_revenue}")
