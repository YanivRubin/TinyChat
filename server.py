#!/usr/bin/env python
"""
TinyChat Server - mediates messages between master and slave clients.

Usage: python server.py [--host HOST] [--port PORT]
"""

import argparse
import os
import select
import socket
import time
from typing import Any, Dict, List, Optional, Text

import six

from protocol import (
    ROLE_MASTER,
    ROLE_SLAVE,
    TYPE_AWAIT,
    TYPE_REGISTER,
    TYPE_SEND,
    decode_message,
    encode_log_entry,
    encode_message,
    make_ack,
    make_error,
    make_log_entry,
    make_message,
    make_registered,
)


class TinyChatServer:
    def __init__(self, host="localhost", port=8765):
        # type: (Text, int) -> None
        self.host = host
        self.port = port
        self.server_socket = None  # type: Optional[socket.socket]
        self.clients = {}  # type: Dict[Text, Dict[str, Any]]
        self.message_queues = {ROLE_MASTER: [], ROLE_SLAVE: []}  # type: Dict[Text, List[Dict[str, Any]]]
        self.log_file = None  # type: Optional[Any]
        self.running = False  # type: bool
        self._unregistered_buffers = {}  # type: Dict[socket.socket, Text]

    def start(self):
        # type: () -> None
        """Start the server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.server_socket.setblocking(False)

        # Create log directory and file with timestamp
        log_dir = os.path.join(os.getcwd(), "logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(log_dir, "chat_{}.log".format(timestamp))
        self.log_file = open(log_filename, "wb")
        print("Server started on {}:{}".format(self.host, self.port))
        print("Logging to: {}".format(log_filename))

        self.running = True
        self.run_loop()

    def run_loop(self):
        # type: () -> None
        """Main event loop using select."""
        assert self.server_socket is not None
        inputs = []  # type: List[socket.socket]
        inputs.append(self.server_socket)
        outputs = []  # type: List[socket.socket]

        while self.running:
            try:
                readable, writable, exceptional = select.select(inputs, outputs, inputs, 1.0)
            except (OSError, ValueError):
                # Clean up any closed sockets from inputs
                inputs = [s for s in inputs if s.fileno() != -1]
                outputs = [s for s in outputs if s.fileno() != -1]
                continue

            for s in readable:
                if s is self.server_socket:
                    self.handle_new_connection(inputs)
                else:
                    self.handle_client_read(s, inputs, outputs)

            for s in writable:
                self.handle_client_write(s)

            for s in exceptional:
                self.handle_exception(s, inputs, outputs)

    def handle_new_connection(self, inputs):
        # type: (List[socket.socket]) -> None
        """Accept a new client connection."""
        assert self.server_socket is not None
        client_socket, addr = self.server_socket.accept()
        client_socket.setblocking(False)
        inputs.append(client_socket)
        # Initialize buffer for unregistered client
        if not hasattr(self, "_unregistered_buffers"):
            self._unregistered_buffers = {}
        self._unregistered_buffers[client_socket] = six.text_type()
        print("New connection from {}".format(addr))

    def handle_client_read(self, client_socket, inputs, outputs):
        # type: (socket.socket, List[socket.socket], List[socket.socket]) -> None
        """Read data from a client."""
        try:
            data = client_socket.recv(4096)
            if not data:
                self.disconnect_client(client_socket, inputs, outputs)
                return

            # Find client role
            role = self.get_role_by_socket(client_socket)
            if role is None:
                # Not registered yet, buffer the data
                self.buffer_data(client_socket, data)
                self.try_register(client_socket)
            else:
                self.buffer_data(client_socket, data)
                self.process_client_messages(role)

        except OSError as e:
            print("Error reading from client: {}".format(e))
            self.disconnect_client(client_socket, inputs, outputs)

    def handle_client_write(self, client_socket):
        # type: (socket.socket) -> None
        """Handle writable client (not used currently, but kept for completeness)."""

    def handle_exception(self, client_socket, inputs, outputs):
        # type: (socket.socket, List[socket.socket], List[socket.socket]) -> None
        """Handle exceptional condition on a socket."""
        print("Exception on socket, disconnecting")
        self.disconnect_client(client_socket, inputs, outputs)

    def get_role_by_socket(self, client_socket):
        # type: (socket.socket) -> Optional[Text]
        """Get the role associated with a socket."""
        for role, info in six.iteritems(self.clients):
            if info["socket"] is client_socket:
                return role
        return None

    def buffer_data(self, client_socket, data):
        # type: (socket.socket, bytes) -> None
        """Add data to client's buffer."""
        role = self.get_role_by_socket(client_socket)
        if role:
            self.clients[role]["buffer"] += data.decode("utf-8")
        else:
            # For unregistered clients, store buffer in a dict keyed by socket
            if not hasattr(self, "_unregistered_buffers"):
                self._unregistered_buffers = {}
            if client_socket not in self._unregistered_buffers:
                self._unregistered_buffers[client_socket] = six.text_type()
            self._unregistered_buffers[client_socket] += data.decode("utf-8")

    def try_register(self, client_socket):
        # type: (socket.socket) -> None
        """Try to register an unregistered client."""
        buffer = self._unregistered_buffers.get(client_socket, six.text_type())
        lines = buffer.split("\n")
        for line in lines[:-1]:  # Process complete lines
            line = line.strip()
            if not line:
                continue
            try:
                msg = decode_message(line.encode("utf-8"))
                if msg.get("type") == TYPE_REGISTER:
                    role = msg.get("role")
                    if role not in (ROLE_MASTER, ROLE_SLAVE):
                        self.send_raw(client_socket, make_error("Invalid role: {}".format(role)))
                        return
                    if role in self.clients:
                        self.send_raw(client_socket, make_error("Role {} already taken".format(role)))
                        return
                    # Register the client
                    self.clients[role] = {
                        "socket": client_socket,
                        "buffer": lines[-1],  # Remaining partial line
                        "awaiting": False,
                    }
                    # Remove from unregistered buffers
                    if client_socket in self._unregistered_buffers:
                        del self._unregistered_buffers[client_socket]
                    self.send_raw(client_socket, make_registered(role))
                    print("Registered {} client".format(role))
                    # Check if there are queued messages for this role
                    self.deliver_queued_messages(role)
                    return
            except (ValueError, KeyError) as e:
                print("Registration error: {}".format(e))
                self.send_raw(client_socket, make_error("Invalid registration: {}".format(e)))
                return
        # Update buffer with remaining partial line
        self._unregistered_buffers[client_socket] = lines[-1]

    def process_client_messages(self, role):
        # type: (Text) -> None
        """Process complete messages from a registered client's buffer."""
        client = self.clients[role]
        buffer = client["buffer"]
        lines = buffer.split("\n")
        client["buffer"] = lines[-1]  # Keep partial line

        for line in lines[:-1]:
            line = line.strip()
            if not line:
                continue
            try:
                msg = decode_message(line.encode("utf-8"))
                self.handle_message(role, msg)
            except (ValueError, KeyError) as e:
                print("Error processing message from {}: {}".format(role, e))
                self.send_to_role(role, make_error("Invalid message: {}".format(e)))

    def handle_message(self, from_role, msg):
        # type: (Text, Dict[str, Any]) -> None
        """Handle a message from a registered client."""
        msg_type = msg.get("type")

        if msg_type == TYPE_SEND:
            self.handle_send(from_role, msg)
        elif msg_type == TYPE_AWAIT:
            self.handle_await(from_role)
        else:
            self.send_to_role(from_role, make_error("Unknown message type: {}".format(msg_type)))

    def handle_send(self, from_role, msg):
        # type: (Text, Dict[str, Any]) -> None
        """Handle a send message - queue for the other role."""
        to_role = ROLE_SLAVE if from_role == ROLE_MASTER else ROLE_MASTER
        message = msg.get("message", six.text_type())
        msg_id = msg.get("msg_id")

        # Log the message
        self.log_message(from_role, to_role, message, msg_id)

        # Queue the message for the recipient
        self.message_queues[to_role].append({
            "message": message,
            "msg_id": msg_id,
            "from": from_role,
        })

        # Acknowledge to sender
        self.send_to_role(from_role, make_ack(msg_id))

        # Try to deliver if recipient is awaiting
        self.deliver_queued_messages(to_role)

    def handle_await(self, role):
        # type: (Text) -> None
        """Handle an await message - mark client as awaiting and deliver queued messages."""
        self.clients[role]["awaiting"] = True
        self.deliver_queued_messages(role)

    def deliver_queued_messages(self, role):
        # type: (Text) -> None
        """Deliver queued messages to a client if they are awaiting."""
        if role not in self.clients:
            return
        if not self.clients[role]["awaiting"]:
            return
        if not self.message_queues[role]:
            return

        # Deliver the first queued message
        queued = self.message_queues[role].pop(0)
        self.send_to_role(role, make_message(
            queued["message"], queued["msg_id"], queued["from"]
        ))
        self.clients[role]["awaiting"] = False

    def send_to_role(self, role, msg):
        # type: (Text, Dict[str, Any]) -> None
        """Send a message to a registered client by role."""
        if role in self.clients:
            self.send_raw(self.clients[role]["socket"], msg)

    def send_raw(self, sock, msg):
        # type: (socket.socket, Dict[str, Any]) -> None
        """Send a raw message to a socket."""
        try:
            sock.sendall(encode_message(msg))
        except OSError as e:
            print("Error sending to client: {}".format(e))

    def log_message(self, from_role, to_role, message, msg_id):
        # type: (Text, Text, Text, Text) -> None
        """Log a message to the log file."""
        if self.log_file:
            direction = "{}->{}".format(from_role, to_role)
            entry = make_log_entry(direction, message, msg_id, from_role, to_role)
            self.log_file.write(encode_log_entry(entry))
            self.log_file.flush()

    def disconnect_client(self, client_socket, inputs=None, outputs=None):
        # type: (socket.socket, Optional[List[socket.socket]], Optional[List[socket.socket]]) -> None
        """Disconnect a client."""
        role = self.get_role_by_socket(client_socket)
        if role:
            print("{} disconnected".format(role))
            del self.clients[role]
        else:
            # Unregistered client
            if hasattr(self, "_unregistered_buffers") and client_socket in self._unregistered_buffers:
                del self._unregistered_buffers[client_socket]

        try:
            client_socket.close()
        except OSError:
            pass

        if inputs and client_socket in inputs:
            inputs.remove(client_socket)
        if outputs and client_socket in outputs:
            outputs.remove(client_socket)

    def stop(self):
        # type: () -> None
        """Stop the server."""
        self.running = False
        for info in six.itervalues(self.clients):
            try:
                info["socket"].close()
            except OSError:
                pass
        if self.server_socket:
            self.server_socket.close()
        if self.log_file:
            self.log_file.close()
        print("Server stopped")


def main():
    parser = argparse.ArgumentParser(description="TinyChat Server")
    parser.add_argument("--host", default="localhost", help="Host to bind to (default: localhost)")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind to (default: 8765)")
    args = parser.parse_args()

    server = TinyChatServer(args.host, args.port)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()


if __name__ == "__main__":
    main()