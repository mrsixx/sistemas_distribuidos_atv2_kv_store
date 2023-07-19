import json
from message import Message
from typing import List, Dict

def json_serialize(message: Message) -> str:
    return json.dumps(message, default=Message.to_json)

def json_deserialize(json_str: str) -> Message:
    if str == '':
        return {}
    return json.loads(json_str, object_hook=Message.from_json)

def socket_receive_all(socket) -> bytes:
    buffer_size = 1024
    content = b""
    while True:
        temp = socket.recv(buffer_size)
        content += temp
        if len(temp) < buffer_size:
            break
    return content
