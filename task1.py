"""Stop-and-Wait ARQ Server for file uploads"""

import argparse
import socket
import logging
import os

from dataclasses import dataclass


# Constants

RCVBUF_SIZE = 20480  # 20 Kib
HOST = "0.0.0.0"


class ServerAlreadyInitializedException(Exception):
    """Occurs if server has been already setup"""


@dataclass
class FileTransmition:
    """Represents file or part of file transmitted by client"""

    filename: str
    filesize: int
    seqno: int
    data: bytearray


class Server:
    """Stop-and-Wait ARQ Server"""

    def __init__(self, port: int, max_clients: int):
        self.port = port
        self.max_clients = max_clients
        self.socket = None
        self.clients = {}

    def setup(self):
        """Setup server

        This function bind socket to local port and prepares internal
        data structures
        """
        if self.socket:
            raise ServerAlreadyInitializedException()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.socket.bind((HOST, self.port))
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_SIZE)

    def run(self):
        """Run server until error or KeyboardInterrupt occurs"""
        logging.info("Server started at %s:%d", HOST, self.port)

        while True:
            try:
                message, address = self.socket.recvfrom(RCVBUF_SIZE)
            except KeyboardInterrupt:
                logging.fatal('Exiting due to keyboard interrupt')
                exit(0)

            # Skip empty packets
            if len(message) == 0:
                continue

            # Process start of transmition
            if message[0] == ord("s"):
                logging.debug("Received starting message from %s", address)

                if len(self.clients) < self.max_clients:
                    _, _, filename, filesize = message.split(b"|", 4)
                    filename = filename.decode()
                    filesize = int(filesize.decode())

                    self.clients[address] = FileTransmition(
                        filename=filename, seqno=0, filesize=filesize, data=bytearray()
                    )

                    logging.debug(
                        'Client is uploading "%s" (%d bytes)', filename, filesize
                    )

                    aseqno = (self.clients[address].seqno + 1) % 2

                    message = b"a|" + str(aseqno).encode()
                    self.socket.sendto(message, address)

                else:
                    self.socket.sendto(b"n|1", address)

            elif message[0] == ord("d"):
                _, seqno, chunk = message.split(b"|", 2)
                seqno = int(seqno.decode())
                if not self.clients.get(address):
                    logging.error("Unknown client sent data packet from %s", address)
                    continue

                if seqno != (self.clients[address].seqno + 1) % 2:
                    logging.error("Client %s sent packet with wrong seqno", address)
                    continue

                self.clients[address].data += bytearray(chunk)
                self.clients[address].seqno = seqno

                aseqno = (self.clients[address].seqno + 1) % 2

                message = b"a|" + str(aseqno).encode()
                self.socket.sendto(message, address)

                if self.clients[address].filesize == len(self.clients[address].data):
                    logging.info(
                        'Client %s finished transmittion of "%s"',
                        address,
                        self.clients[address].filename,
                    )

                    file_transfer = self.clients[address]

                    if os.path.exists(file_transfer.filename):
                        logging.warning(
                            "File with such filename already exists. It will be overwritten"
                        )

                    with open(file_transfer.filename, "wb+") as outfile:
                        outfile.write(file_transfer.data)

                    logging.warning(
                        'File "%s" uploaded sucessfully', file_transfer.filename
                    )

                    self.clients.pop(address)

            else:
                logging.error("Received malformed message from %s", address)
                logging.debug("Malformed message: %s", message)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    parser.add_argument("maxclients")

    args = parser.parse_args()

    server = Server(int(args.port), int(args.maxclients))
    server.setup()
    server.run()
