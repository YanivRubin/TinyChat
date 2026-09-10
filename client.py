#!/usr/bin/env python
"""
TinyChat Client - send messages to or await messages from the other party.

Usage:
    client.py <master|slave> send "MESSAGE"
    client.py <master|slave> await
"""

import argparse
import json
import socket
import sys
import time
from typing import Any, Dict, Optional, Text, Union

import six

from protocol import (
    TYPE_ACK,
    TYPE_ERROR,
    TYPE_MESSAGE,
    TYPE_REGISTERED,
    decode_message,
    encode_message,
    generate_msg_id,
    make_await,
    make_register,
    make_send,
)


class TinyChatClient:
    def __init__(self, role, host="localhost", port=8765, timeout=5):
        # type: (Text, Text, int, float) -> None
        self.role = role
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None  # type: Optional[socket.socket]
        self.buffer = ""  # type: Text

    def connect(self):
        # type: () -> bool
        """Connect to the server and register."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.socket.connect((self.host, self.port))
        self.socket.setblocking(False)

        # Register
        self.send_raw(make_register(self.role))

        # Wait for registration confirmation
        start = time.time()
        while time.time() - start < self.timeout:
            msg = self.receive_message()
            if msg:
                if msg.get("type") == TYPE_REGISTERED:
                    print("Connected as {}".format(self.role))
                    return True
                elif msg.get("type") == TYPE_ERROR:
                    print("Registration failed: {}".format(msg.get("message")))
                    return False
            time.sleep(0.01)

        print("Registration timeout")
        return False

    def send_raw(self, msg):
        # type: (Dict[str, Any]) -> None
        """Send a raw message."""
        assert self.socket is not None
        self.socket.sendall(encode_message(msg))

    def receive_message(self):
        # type: () -> Optional[Dict[str, Any]]
        """Try to receive a complete message. Returns dict or None."""
        assert self.socket is not None
        try:
            data = self.socket.recv(4096)
            if not data:
                return None
            self.buffer += data.decode("utf-8")
        except (OSError, socket.error):
            # No data available (non-blocking)
            return None

        # Process complete lines
        lines = self.buffer.split("\n")
        self.buffer = lines[-1]  # Keep partial line

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue
            try:
                return decode_message(line.encode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print("Decode error: {}".format(e))
                return None
        return None

    def send_message(self, message):
        # type: (Text) -> bool
        """Send a message and wait for ACK."""
        msg_id = generate_msg_id()
        self.send_raw(make_send(message, msg_id))

        # Wait for ACK
        start = time.time()
        while time.time() - start < self.timeout:
            msg = self.receive_message()
            if msg:
                if msg.get("type") == TYPE_ACK and msg.get("msg_id") == msg_id:
                    return True
                elif msg.get("type") == TYPE_ERROR:
                    print("Error: {}".format(msg.get("message")))
                    return False
            time.sleep(0.01)

        print("Send timeout (no ACK)")
        return False

    def await_message(self):
        # type: () -> Optional[Text]
        """Wait for a message from the other party. Blocks until received."""
        self.send_raw(make_await())

        # Wait for message
        while True:
            msg = self.receive_message()
            if msg:
                if msg.get("type") == TYPE_MESSAGE:
                    return msg.get("message")
                elif msg.get("type") == TYPE_ERROR:
                    print("Error: {}".format(msg.get("message")))
                    return None
            time.sleep(0.01)

    def close(self):
        # type: () -> None
        """Close the connection."""
        if self.socket:
            self.socket.close()
            self.socket = None


def main():
    parser = argparse.ArgumentParser(
        description="TinyChat Client",
        usage="%(prog)s <master|slave> send \"MESSAGE\"\n       %(prog)s <master|slave> await"
    )
    parser.add_argument("role", choices=["master", "slave"], help="Role: master or slave")
    parser.add_argument("command", choices=["send", "await"], help="Command: send or await")
    parser.add_argument("message", nargs="?", help="Message to send (required for send command)")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=8765, help="Server port (default: 8765)")

    args = parser.parse_args()

    if args.command == "send" and args.message is None:
        parser.error("send command requires a message argument")

    client = TinyChatClient(args.role, args.host, args.port)

    try:
        if not client.connect():
            sys.exit(1)

        if args.command == "send":
            success = client.send_message(args.message)
            sys.exit(0 if success else 1)
        elif args.command == "await":
            message = client.await_message()
            if message is not None:
                print(message)
            sys.exit(0 if message is not None else 1)

    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()