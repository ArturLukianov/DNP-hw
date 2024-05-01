import zmq
import json
import sys
import threading
from datetime import datetime, timedelta

WEATHER_INPUT_PORT = 5555
FASHION_SOCKET_PORT = 5556

IP_ADDR = "127.0.0.1"

latest_data = {}


def average_temperature_humidity():
    last_30s_data = latest_data["weather_data"][-15:]
    latest_data["average-temp"] = sum((x["temperature"] for x in last_30s_data)) / len(
        last_30s_data
    )
    latest_data["average-hum"] = sum((x["humidity"] for x in last_30s_data)) / len(
        last_30s_data
    )


def recommendation():
    result = ""
    average_temperature_humidity()
    if latest_data["average-temp"] < 10:
        result = "Today weather is cold. Its better to wear warm clothes"
    elif latest_data["average-temp"] > 10 and latest_data["average-temp"] < 25:
        result = "Feel free to wear spring/autumn clothes"
    else:
        result = "Go for light clothes"
    print(result)
    return result


def report():
    average_temperature_humidity()
    result = f"The last 30 sec average Temperature is {latest_data['average-temp']} and Humidity {latest_data['average-hum']}"
    print(result)
    return result


def main():
    """Main cycle"""
    is_running = True
    log_file = open("weather_data.log", "a+")
    context = zmq.Context()
    weather_socket = context.socket(zmq.SUB)
    weather_socket.setsockopt_string(zmq.SUBSCRIBE, "weather")
    weather_socket.connect(f"tcp://{IP_ADDR}:{WEATHER_INPUT_PORT}")

    fashion_socket = context.socket(zmq.REP)
    fashion_socket.bind(f"tcp://{IP_ADDR}:{FASHION_SOCKET_PORT}")

    poller = zmq.Poller()
    poller.register(weather_socket, zmq.POLLIN)
    poller.register(fashion_socket, zmq.POLLIN)

    latest_data["weather_data"] = []

    while is_running:
        sockets = dict(poller.poll())
        if weather_socket in sockets and sockets[weather_socket] == zmq.POLLIN:
            weather_data = json.loads(weather_socket.recv().decode()[7:])
            print(weather_data)

            latest_data["weather_data"].append(weather_data)

            # Log to file
            log_file.write(json.dumps(weather_data) + "\n")
            log_file.flush()

        if fashion_socket in sockets and sockets[fashion_socket] == zmq.POLLIN:
            # Handle client
            fashion_data = fashion_socket.recv().decode()
            if fashion_data == "Weather":
                # Handle weather
                average_temperature_humidity()
                avg_temp = latest_data['average-temp']
                avg_hum = latest_data['average-hum']
                response = (
                    f"The last 30 sec average Temperature is {avg_temp} and Humidity {avg_hum}"
                )
                fashion_socket.send_string(response)
            elif fashion_data == "Fashion":
                # Handle fashion
                response = recommendation()
                fashion_socket.send_string(response)

    log_file.close()


if __name__ == "__main__":
    main()
