import zmq
import json
import sys

WEATHER_INPUT_PORT = 5555
IP_ADDR = "127.0.0.1"


def main():
    """Main cycle"""
    is_running = True
    log_file = open("co2_data.log", "a+")
    context = zmq.Context()
    weather_socket = context.socket(zmq.SUB)
    weather_socket.setsockopt_string(zmq.SUBSCRIBE, "co2")
    weather_socket.connect(f"tcp://{IP_ADDR}:{WEATHER_INPUT_PORT}")

    while is_running:
        co2_data = json.loads(weather_socket.recv().decode()[4:])
        print(co2_data)

        # Log to file
        log_file.write(json.dumps(co2_data) + "\n")
        log_file.flush()

        # Strictly higher, as said in task description
        if co2_data['co2'] > 400:
            print("Danger Zone! Please do not leave home")



if __name__ == "__main__":
    main()
