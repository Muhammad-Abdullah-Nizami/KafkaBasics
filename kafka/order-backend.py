import json
import time


from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

#producer = KafkaProducer(bootstrap_servers="localhost:29092")
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
              api_version=(0,11,5))
print("Going to generate one unique order every 5 seconds.")

for i in range (1, ORDER_LIMIT):
    data = {
        "order-id": i,
        "user-id": f"nizami_{i}",
        "total-cost": i*2,
        "items": "eggs, bread"
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    print (f"done sending..{i}")
    time.sleep(5)