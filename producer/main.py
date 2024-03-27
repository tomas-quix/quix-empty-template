import psutil
import os
import time
import json
from quixstreams import Application

from dotenv import load_dotenv
load_dotenv()

def get_cpu_load():
    cpu_load = psutil.cpu_percent(interval=1)
    return cpu_load

app = Application(broker_address="localhost:9092")
output_topic = app.topic("cpu-data")

def main():
    with app.get_producer() as producer:
        while True:        
            cpu_load = get_cpu_load()
            print("CPU load: ", cpu_load)
            timestamp = int(time.time_ns()) # Quix timestamp is nano seconds
            message = {"timestamp": timestamp, "cpu_load": cpu_load}

            producer.produce(
                topic=output_topic.name,
                key="server-1-cpu",
                value=json.dumps(message)
            )

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Exiting due to keyboard interrupt')