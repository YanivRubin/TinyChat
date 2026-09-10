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
import uuid
from datetime import datetime, timezone
from typing import TypedDict

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
ClientMessage = RegisterMessage | SendMessage | AwaitMessage
# Union type for all server->client messages
ServerMessage = RegisteredMessage | DeliveryMessage | AckMessage | ErrorMessage


def generate_msg_id():
    # type: () -> str
    """Generate a unique message ID."""
    return str(uuid.uuid4())[:8]


def make_register(role):
    # type: (str) -> dict
    """Create a register message."""
    return {"type": TYPE_REGISTER, "role": role}


def make_send(message, msg_id=None):
    # type: (str, str) -> dict
    """Create a send message."""
    return {"type": TYPE_SEND, "message": message, "msg_id": msg_id or generate_msg_id()}


def make_await():
    # type: () -> dict
    """Create an await message."""
    return {"type": TYPE_AWAIT}


def make_registered(role):
    # type: (str) -> dict
    """Create a registered response."""
    return {"type": TYPE_REGISTERED, "role": role}


def make_message(message, msg_id, from_role):
    # type: (str, str, str) -> dict
    """Create a message delivery."""
    return {"type": TYPE_MESSAGE, "message": message, "msg_id": msg_id, "from": from_role}


def make_ack(msg_id):
    # type: (str) -> dict
    """Create an acknowledgment."""
    return {"type": TYPE_ACK, "msg_id": msg_id}


def make_error(message):
    # type: (str) -> dict
    """Create an error response."""
    return {"type": TYPE_ERROR, "message": message}


def encode_message(msg):
    # type: (dict) -> bytes
    """Encode a message dict to JSON line bytes."""
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode_message(line):
    # type: (bytes) -> dict
    """Decode a JSON line to message dict."""
    return json.loads(line.decode("utf-8").strip())


def make_log_entry(direction, message, msg_id, from_role, to_role):
    # type: (str, str, str, str, str) -> dict
    """Create a log entry dict."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "direction": direction,  # "master->slave" or "slave->master"
        "message": message,
        "msg_id": msg_id,
        "from": from_role,
        "to": to_role,
    }


def encode_log_entry(entry):
    # type: (dict) -> bytes
    """Encode a log entry to JSON line bytes."""
    return (json.dumps(entry) + "\n").encode("utf-8")