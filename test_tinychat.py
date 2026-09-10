#!/usr/bin/env python
"""
Tests for TinyChat server and client.
"""

import json
import socket
import subprocess
import time
import sys

import six

import pytest

from protocol import (
    ROLE_MASTER,
    ROLE_SLAVE,
    TYPE_ACK,
    TYPE_AWAIT,
    TYPE_ERROR,
    TYPE_MESSAGE,
    TYPE_REGISTER,
    TYPE_REGISTERED,
    TYPE_SEND,
    decode_message,
    encode_log_entry,
    encode_message,
    generate_msg_id,
    make_ack,
    make_await,
    make_error,
    make_log_entry,
    make_message,
    make_register,
    make_registered,
    make_send,
)

# Python 2/3 compatibility for exception types
if six.PY2:
    ConnectionError = socket.error
    BlockingIOError = socket.error
    TimeoutError = socket.error
else:
    ConnectionError = ConnectionError
    BlockingIOError = BlockingIOError
    TimeoutError = TimeoutError

# =============================================================================
# Protocol Tests
# =============================================================================

def test_generate_msg_id():
    """Test message ID generation."""
    msg_id = generate_msg_id()
    assert isinstance(msg_id, six.string_types)
    assert len(msg_id) == 8
    # Should be unique
    msg_id2 = generate_msg_id()
    assert msg_id != msg_id2


def test_make_register():
    """Test register message creation."""
    msg = make_register(ROLE_MASTER)
    assert msg["type"] == TYPE_REGISTER
    assert msg["role"] == ROLE_MASTER


def test_make_send():
    """Test send message creation."""
    msg = make_send("hello", "abc123")
    assert msg["type"] == TYPE_SEND
    assert msg["message"] == "hello"
    assert msg["msg_id"] == "abc123"
    # Test auto-generated ID
    msg2 = make_send("world")
    assert "msg_id" in msg2
    assert len(msg2["msg_id"]) == 8


def test_make_await():
    """Test await message creation."""
    msg = make_await()
    assert msg["type"] == TYPE_AWAIT


def test_make_registered():
    """Test registered response creation."""
    msg = make_registered(ROLE_SLAVE)
    assert msg["type"] == TYPE_REGISTERED
    assert msg["role"] == ROLE_SLAVE


def test_make_message():
    """Test message delivery creation."""
    msg = make_message("hello", "abc123", ROLE_MASTER)
    assert msg["type"] == TYPE_MESSAGE
    assert msg["message"] == "hello"
    assert msg["msg_id"] == "abc123"
    assert msg["from"] == ROLE_MASTER


def test_make_ack():
    """Test acknowledgment creation."""
    msg = make_ack("abc123")
    assert msg["type"] == TYPE_ACK
    assert msg["msg_id"] == "abc123"


def test_make_error():
    """Test error response creation."""
    msg = make_error("something went wrong")
    assert msg["type"] == TYPE_ERROR
    assert msg["message"] == "something went wrong"


def test_encode_decode_message():
    """Test message encoding/decoding round-trip."""
    original = {"type": "send", "message": "hello", "msg_id": "abc123"}
    encoded = encode_message(original)
    decoded = decode_message(encoded)
    assert decoded == original


def test_encode_decode_log_entry():
    """Test log entry encoding/decoding round-trip."""
    entry = make_log_entry("master->slave", "hello", "abc123", ROLE_MASTER, ROLE_SLAVE)
    encoded = encode_log_entry(entry)
    decoded = decode_message(encoded)
    assert decoded == entry


def test_make_log_entry_format():
    """Test log entry has correct format."""
    entry = make_log_entry("master->slave", "hello", "abc123", ROLE_MASTER, ROLE_SLAVE)
    assert "timestamp" in entry
    assert entry["direction"] == "master->slave"
    assert entry["message"] == "hello"
    assert entry["msg_id"] == "abc123"
    assert entry["from"] == ROLE_MASTER
    assert entry["to"] == ROLE_SLAVE
    # Timestamp should end with Z (UTC)
    assert entry["timestamp"].endswith("Z")


# =============================================================================
# Integration Tests (require running server)
# =============================================================================

class TestIntegration:
    """Integration tests with running server."""

    SERVER_HOST = "localhost"
    SERVER_PORT = 18765  # Use non-standard port to avoid conflicts
    SERVER_PROC = None
    LOG_FILES = []
    _log_files = []

    @classmethod
    def setup_class(cls):
        """Start server before all tests."""
        cls.SERVER_PROC = subprocess.Popen(
            [sys.executable, "server.py", "--host", cls.SERVER_HOST, "--port", str(cls.SERVER_PORT)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Wait for server to start
        time.sleep(1)
        # Verify server is up
        for _ in range(10):
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect((cls.SERVER_HOST, cls.SERVER_PORT))
                s.close()
                break
            except (ConnectionError, socket.error):
                time.sleep(0.1)
        else:
            raise RuntimeError("Server failed to start")

    @classmethod
    def teardown_class(cls):
        """Stop server after all tests."""
        if cls.SERVER_PROC:
            cls.SERVER_PROC.terminate()
            # Python 2: wait() doesn't accept timeout
            if six.PY2:
                cls.SERVER_PROC.wait()
            else:
                cls.SERVER_PROC.wait(timeout=5)
        # Clean up log files
        for log_file in cls._log_files:
            try:
                import os
                os.remove(log_file)
            except OSError:
                pass

    def _connect_and_register(self, role):
        """Helper to connect and register as a role."""
        s = socket.socket()
        s.settimeout(5)
        s.connect((self.SERVER_HOST, self.SERVER_PORT))
        s.setblocking(False)
        s.sendall(encode_message(make_register(role)))
        # Wait for registered response
        start = time.time()
        while time.time() - start < 5:
            try:
                data = s.recv(4096)
                if data:
                    msg = decode_message(data)
                    if msg.get("type") == TYPE_REGISTERED:
                        return s
                    elif msg.get("type") == TYPE_ERROR:
                        raise RuntimeError("Registration failed: {}".format(msg.get("message")))
            except (BlockingIOError, socket.error):
                time.sleep(0.01)
        raise RuntimeError("Registration timeout")

    def _send_raw(self, sock, msg):
        """Send a message and wait for response."""
        sock.sendall(encode_message(msg))
        time.sleep(0.1)

    def _recv_message(self, sock):
        """Receive a complete message."""
        sock.settimeout(5)
        buffer = six.text_type()
        start = time.time()
        while time.time() - start < 5:
            try:
                data = sock.recv(4096)
                if not data:
                    return None
                buffer += data.decode("utf-8")
                lines = buffer.split("\n")
                buffer = lines[-1]
                for line in lines[:-1]:
                    line = line.strip()
                    if line:
                        return decode_message(line.encode("utf-8"))
            except socket.timeout:
                continue
        return None

    def test_slave_await_master_send(self):
        """Test slave await -> master send flow."""
        # Slave connects and awaits
        slave_sock = self._connect_and_register(ROLE_SLAVE)
        self._send_raw(slave_sock, make_await())

        # Master connects and sends
        master_sock = self._connect_and_register(ROLE_MASTER)
        msg_id = generate_msg_id()
        self._send_raw(master_sock, make_send("hello from master", msg_id))

        # Slave should receive message
        msg = self._recv_message(slave_sock)
        assert msg is not None
        assert msg["type"] == TYPE_MESSAGE
        assert msg["message"] == "hello from master"
        assert msg["from"] == ROLE_MASTER

        # Master should receive ACK
        ack = self._recv_message(master_sock)
        assert ack is not None
        assert ack["type"] == TYPE_ACK
        assert ack["msg_id"] == msg_id

        slave_sock.close()
        master_sock.close()

    def test_master_await_slave_send(self):
        """Test master await -> slave send flow."""
        # Master connects and awaits
        master_sock = self._connect_and_register(ROLE_MASTER)
        self._send_raw(master_sock, make_await())

        # Slave connects and sends
        slave_sock = self._connect_and_register(ROLE_SLAVE)
        msg_id = generate_msg_id()
        self._send_raw(slave_sock, make_send("hello from slave", msg_id))

        # Master should receive message
        msg = self._recv_message(master_sock)
        assert msg is not None
        assert msg["type"] == TYPE_MESSAGE
        assert msg["message"] == "hello from slave"
        assert msg["from"] == ROLE_SLAVE

        # Slave should receive ACK
        ack = self._recv_message(slave_sock)
        assert ack is not None
        assert ack["type"] == TYPE_ACK
        assert ack["msg_id"] == msg_id

        master_sock.close()
        slave_sock.close()

    def test_queued_message_master_to_slave(self):
        """Test message queuing: master sends before slave awaits."""
        # Master connects and sends
        master_sock = self._connect_and_register(ROLE_MASTER)
        msg_id = generate_msg_id()
        self._send_raw(master_sock, make_send("queued message", msg_id))

        # Wait for ACK
        ack = self._recv_message(master_sock)
        assert ack["type"] == TYPE_ACK
        master_sock.close()

        # Slave connects and awaits - should get message immediately
        slave_sock = self._connect_and_register(ROLE_SLAVE)
        self._send_raw(slave_sock, make_await())

        msg = self._recv_message(slave_sock)
        assert msg is not None
        assert msg["type"] == TYPE_MESSAGE
        assert msg["message"] == "queued message"
        assert msg["from"] == ROLE_MASTER
        assert msg["msg_id"] == msg_id

        slave_sock.close()

    def test_queued_message_slave_to_master(self):
        """Test message queuing: slave sends before master awaits."""
        # Slave connects and sends
        slave_sock = self._connect_and_register(ROLE_SLAVE)
        msg_id = generate_msg_id()
        self._send_raw(slave_sock, make_send("queued from slave", msg_id))

        # Wait for ACK
        ack = self._recv_message(slave_sock)
        assert ack["type"] == TYPE_ACK
        slave_sock.close()

        # Master connects and awaits - should get message immediately
        master_sock = self._connect_and_register(ROLE_MASTER)
        self._send_raw(master_sock, make_await())

        msg = self._recv_message(master_sock)
        assert msg is not None
        assert msg["type"] == TYPE_MESSAGE
        assert msg["message"] == "queued from slave"
        assert msg["from"] == ROLE_SLAVE
        assert msg["msg_id"] == msg_id

        master_sock.close()

    def test_duplicate_role_rejected(self):
        """Test that duplicate role registration is rejected."""
        sock1 = self._connect_and_register(ROLE_MASTER)
        # Try to register another master
        sock2 = socket.socket()
        sock2.settimeout(5)
        sock2.connect((self.SERVER_HOST, self.SERVER_PORT))
        sock2.setblocking(False)
        sock2.sendall(encode_message(make_register(ROLE_MASTER)))

        # Should get error
        start = time.time()
        error_received = False
        while time.time() - start < 5:
            try:
                data = sock2.recv(4096)
                if data:
                    msg = decode_message(data)
                    if msg.get("type") == TYPE_ERROR:
                        assert "already taken" in msg.get("message", "")
                        error_received = True
                        break
            except (BlockingIOError, socket.error):
                time.sleep(0.01)

        assert error_received, "Should have received error for duplicate role"
        sock1.close()
        sock2.close()

    def test_invalid_role_rejected(self):
        """Test that invalid role is rejected."""
        sock = socket.socket()
        sock.settimeout(5)
        sock.connect((self.SERVER_HOST, self.SERVER_PORT))
        sock.setblocking(False)
        sock.sendall(encode_message({"type": "register", "role": "invalid"}))

        start = time.time()
        error_received = False
        while time.time() - start < 5:
            try:
                data = sock.recv(4096)
                if data:
                    msg = decode_message(data)
                    if msg.get("type") == TYPE_ERROR:
                        assert "Invalid role" in msg.get("message", "")
                        error_received = True
                        break
            except (BlockingIOError, socket.error):
                time.sleep(0.01)

        assert error_received, "Should have received error for invalid role"
        sock.close()

    def test_log_file_created(self):
        """Test that log file is created with correct format."""
        # Find the log file in logs/ directory
        import glob
        log_files = glob.glob("logs/chat_*.log")
        assert len(log_files) > 0, "Log file should be created"
        self._log_files.extend(log_files)

        # Read and verify log entries
        with open(log_files[0], "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    entry = json.loads(line)
                    assert "timestamp" in entry
                    assert "direction" in entry
                    assert "message" in entry
                    assert "msg_id" in entry
                    assert "from" in entry
                    assert "to" in entry
                    assert entry["direction"] in ("master->slave", "slave->master")
                    assert entry["from"] in (ROLE_MASTER, ROLE_SLAVE)
                    assert entry["to"] in (ROLE_MASTER, ROLE_SLAVE)


# =============================================================================
# Client Script Tests
# =============================================================================

class TestClientScript:
    """Tests for client.py script via subprocess."""

    SERVER_HOST = "localhost"
    SERVER_PORT = 18766  # Different port for client tests
    SERVER_PROC = None

    @classmethod
    def setup_class(cls):
        """Start server."""
        cls.SERVER_PROC = subprocess.Popen(
            [sys.executable, "server.py", "--host", cls.SERVER_HOST, "--port", str(cls.SERVER_PORT)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(1)

    @classmethod
    def teardown_class(cls):
        """Stop server."""
        if cls.SERVER_PROC:
            cls.SERVER_PROC.terminate()
            if six.PY2:
                cls.SERVER_PROC.wait()
            else:
                cls.SERVER_PROC.wait(timeout=5)

    def run_client(self, role, command, message=None, timeout=10):
        # type: (six.text_type, six.text_type, six.text_type, int) -> tuple
        """Run client.py and return (returncode, stdout, stderr)."""
        cmd = [sys.executable, "client.py", "--host", self.SERVER_HOST, "--port", str(self.SERVER_PORT), role, command]
        if message is not None:
            cmd.append(message)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        try:
            if six.PY2:
                # Python 2: communicate() doesn't accept timeout
                stdout, stderr = proc.communicate()
            else:
                stdout, stderr = proc.communicate(timeout=timeout)
            return proc.returncode, stdout.strip(), stderr.strip()
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            raise

    def test_client_slave_await_master_send(self):
        """Test full flow using client script."""
        # Start slave await in background
        slave_proc = subprocess.Popen(
            [sys.executable, "client.py", "--host", self.SERVER_HOST, "--port", str(self.SERVER_PORT), "slave", "await"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        time.sleep(0.5)

        # Master sends
        ret, _, _ = self.run_client("master", "send", "hello from master")
        assert ret == 0

        # Slave should receive and exit
        slave_stdout, _ = slave_proc.communicate()
        assert slave_proc.returncode == 0
        assert "hello from master" in slave_stdout

    def test_client_master_await_slave_send(self):
        """Test reverse flow using client script."""
        master_proc = subprocess.Popen(
            [sys.executable, "client.py", "--host", self.SERVER_HOST, "--port", str(self.SERVER_PORT), "master", "await"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        time.sleep(0.5)

        ret, _, _ = self.run_client("slave", "send", "hello from slave")
        assert ret == 0

        slave_stdout, _ = master_proc.communicate()
        assert master_proc.returncode == 0
        assert "hello from slave" in slave_stdout

    def test_client_queued_master_to_slave(self):
        """Test queued message via client script."""
        # Master sends first
        ret, _, _ = self.run_client("master", "send", "queued message")
        assert ret == 0

        # Slave awaits - should get message immediately
        ret, out, _ = self.run_client("slave", "await")
        assert ret == 0
        assert "queued message" in out

    def test_client_invalid_role(self):
        """Test client rejects invalid role."""
        ret, _, err = self.run_client("invalid", "send", "test")
        assert ret != 0
        assert "invalid choice" in err

    def test_client_send_requires_message(self):
        """Test client send requires message argument."""
        ret, _, err = self.run_client("master", "send")
        assert ret != 0
        assert "requires a message" in err


if __name__ == "__main__":
    pytest.main([__file__, "-v"])