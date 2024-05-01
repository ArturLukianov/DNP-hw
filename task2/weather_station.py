import sys
import zmq
import time
import json
import random
from datetime import datetime

IP_ADDR = '127.0.0.1'
DATA_PROCESSES_INPUT_PORT = 5555


def random_weather() -> dict:
    """Generate random weather for station"""

    current_date_time = datetime.now()
    now = current_date_time.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "time": now,
        "temperature": round(random.uniform(5, 40), 1),
        "humidity": round(random.uniform(40, 100), 1),
    }


def random_co2() -> dict:
    """Generate random CO2 value for station"""

    current_date_time = datetime.now()
    now = current_date_time.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "time": now,
        "co2": round(random.uniform(300, 500), 1),
    }

def main():
    """Main cycle"""
    is_running = True
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://{IP_ADDR}:{DATA_PROCESSES_INPUT_PORT}")

    while is_running:
        try:
            weather_data = random_weather()
            socket.send_string("weather " + json.dumps(weather_data), )
            print("Weather is sent from WS1", weather_data)
            time.sleep(2)

            co2_data = random_co2()
            socket.send_string("co2 " + json.dumps(co2_data))
            print("CO2 is sent from WS1", co2_data)
            time.sleep(2)
        except KeyboardInterrupt:
            print('Terminating weather station')
            is_running = False

if __name__ == '__main__':
    main()