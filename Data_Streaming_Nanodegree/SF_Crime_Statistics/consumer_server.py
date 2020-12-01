from kafka import KafkaConsumer


def run_consume_server(topic):
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=500,
                             group_id="simple_tester")

    consumer.subscribe([topic])

    for message in consumer:
        print(message.value)



# Entry point ...
if __name__ == "__main__":
    run_consume_server(topic="sfpd.call.log")