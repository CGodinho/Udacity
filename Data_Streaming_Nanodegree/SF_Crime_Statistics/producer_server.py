from kafka import KafkaProducer
import json
import time


# Producer class
class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic


    # Return the json dictionary to binary
    def generate_data(self):
        with open(self.input_file) as line:
            json_array = json.load(line)
            for item in json_array:
                message = self.dict_to_binary(item)
                self.send(self.topic, message)
                time.sleep(0.5)


    # Return the json dictionary to binary                
    def dict_to_binary(self, json_dict) -> bytes:
        return json.dumps(json_dict).encode('utf-8')        