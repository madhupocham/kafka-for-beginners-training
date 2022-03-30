from kafka import KafkaProducer
import json
from data import get_registered_user
import time


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers='kafka-1:9092,kafka-2:9092',
                         value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        future = producer.send("registered_user", registered_user)
        result = future.get(timeout=60)
        print(result)
        time.sleep(4)
