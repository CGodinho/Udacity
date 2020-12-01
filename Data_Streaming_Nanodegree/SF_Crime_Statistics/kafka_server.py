import producer_server


def run_kafka_server():
    # File with call logs
    input_file = "police-department-calls-for-service.json"

    # Create a producer with ProducerServer class to send data
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sfpd.call.log",
        bootstrap_servers="localhost:9092",
        client_id="sender"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


# Entry point ...
if __name__ == "__main__":
    print("Starting ...")
    feed()
    print("End!")
