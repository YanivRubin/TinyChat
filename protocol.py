"""
Wire protocol for TinyChat.

Message types (client -> server):
- REGISTER: {"type": "register", "role": "master|slave"}
- SEND: {"type": "send", "message": "string", "msg_id": "unique_id"}
- AWAIT: {"type": "await"}

Message types (server -> client):
- REGISTERED: {"type": "registered", "role": "master|slave"}
- MESSAGE: {"type": "message", "message": "string", "msg_id": "unique_id", "from": "master|slave"}
- ACK: {"type": "ack", "msg_id": "unique_id"}
- ERROR: {"type": "error", "message": "string"}

Protocol: JSON lines (newline-delimited JSON) over TCP.
"""

import json
import socket
import uuid
from datetime import datetime

import six
from typing import Any, Dict, List, Tuple, Union, TypedDict

# Message type constants
TYPE_REGISTER = "register"
TYPE_SEND = "send"
TYPE_AWAIT = "await"
TYPE_REGISTERED = "registered"
TYPE_MESSAGE = "message"
TYPE_ACK = "ack"
TYPE_ERROR = "error"

ROLE_MASTER = "master"
ROLE_SLAVE = "slave"

# TypedDict definitions for protocol messages (Python 3.8+)
# Use functional syntax to allow reserved keywords as field names
RegisterMessage = TypedDict('RegisterMessage', {
    'type': str,  # "register"
    'role': str,  # "master" | "slave"
})
SendMessage = TypedDict('SendMessage', {
    'type': str,  # "send"
    'message': str,
    'msg_id': str,
})
AwaitMessage = TypedDict('AwaitMessage', {
    'type': str,  # "await"
})
RegisteredMessage = TypedDict('RegisteredMessage', {
    'type': str,  # "registered"
    'role': str,  # "master" | "slave"
})
DeliveryMessage = TypedDict('DeliveryMessage', {
    'type': str,  # "message"
    'message': str,
    'msg_id': str,
    'from': str,  # "master" | "slave"
})
AckMessage = TypedDict('AckMessage', {
    'type': str,  # "ack"
    'msg_id': str,
})
ErrorMessage = TypedDict('ErrorMessage', {
    'type': str,  # "error"
    'message': str,
})
LogEntry = TypedDict('LogEntry', {
    'timestamp': str,
    'direction': str,  # "master->slave" | "slave->master"
    'message': str,
    'msg_id': str,
    'from': str,  # "master" | "slave"
    'to': str,  # "master" | "slave"
})

# Union type for all client->server messages
ClientMessage = Union[RegisterMessage, SendMessage, AwaitMessage]
ServerMessage = Union[RegisteredMessage, DeliveryMessage, AckMessage, ErrorMessage]


def _get_utc_timestamp():
    # type: () -> str
    """Get UTC timestamp in ISO format with Z suffix."""
    # Use utcnow() which works in both Python 2 and 3
    return datetime.utcnow().isoformat() + "Z"


def generate_msg_id():
    # type: () -> str
    """Generate a unique message ID."""
    return str(uuid.uuid4())[:8]


def make_register(role):
    # type: (six.text_type) -> dict
    """Create a register message."""
    return {"type": TYPE_REGISTER, "role": role}


def make_send(message, msg_id=None):
    # type: (six.text_type, six.text_type) -> dict
    """Create a send message."""
    return {"type": TYPE_SEND, "message": message, "msg_id": msg_id or generate_msg_id()}


def make_await():
    # type: () -> dict
    """Create an await message."""
    return {"type": TYPE_AWAIT}


def make_registered(role):
    # type: (six.text_type) -> dict
    """Create a registered response."""
    return {"type": TYPE_REGISTERED, "role": role}


def make_message(message, msg_id, from_role):
    # type: (six.text_type, six.text_type, six.text_type) -> dict
    """Create a message delivery."""
    return {"type": TYPE_MESSAGE, "message": message, "msg_id": msg_id, "from": from_role}


def make_ack(msg_id):
    # type: (six.text_type) -> dict
    """Create an acknowledgment."""
    return {"type": TYPE_ACK, "msg_id": msg_id}


def make_error(message):
    # type: (six.text_type) -> dict
    """Create an error response."""
    return {"type": TYPE_ERROR, "message": message}


def encode_message(msg):
    # type: (dict) -> six.binary_type
    """Encode a message dict to JSON line bytes."""
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode_message(line):
    # type: (six.binary_type) -> dict
    """Decode a JSON line to message dict."""
    return json.loads(line.decode("utf-8").strip())


def make_log_entry(direction, message, msg_id, from_role, to_role):
    # type: (six.text_type, six.text_type, six.text_type, six.text_type, six.text_type) -> dict
    """Create a log entry dict."""
    return {
        "timestamp": _get_utc_timestamp(),
        "direction": direction,  # "master->slave" or "slave->master"
        "message": message,
        "msg_id": msg_id,
        "from": from_role,
        "to": to_role,
    }


def encode_log_entry(entry):
    # type: (dict) -> six.binary_type
    """Encode a log entry to JSON line bytes."""
    return (json.dumps(entry) + "\n").encode("utf-8")

