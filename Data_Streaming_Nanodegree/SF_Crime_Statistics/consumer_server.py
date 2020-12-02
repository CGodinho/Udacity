from kafka import KafkaConsumer
from json import loads


def run_consume_server():
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=500,
                             group_id="simple_tester",
                             value_deserializer=lambda x: loads(x.decode('utf-8')))

    consumer.subscribe(['sfpd.call.log'])

    for message in consumer:
        print(message.value)



# Entry point ...
if __name__ == "__main__":
    run_consume_server()