from kafka import KafkaProducer
import json
import time


# Producer class
class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic


    # Poducing data to the topic
    def generate_data(self):
        counter = 0
        with open(self.input_file) as f:
            for line in f:
                message = self.dict_to_binary(line)
                # Send created message from log, over topic
                self.send(self.topic, message)
                time.sleep(0.1)
                if counter % 100 == 0:
                    print("Message: " + str(counter))
                counter = counter + 1


    # Return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
        