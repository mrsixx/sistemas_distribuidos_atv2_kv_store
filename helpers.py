import json
from socket import socket, AF_INET, SOCK_STREAM
from message import Message
from typing import List, Dict

def json_serialize(obj: Dict) -> str:
    return json.dumps(obj)

def json_deserialize(obj_str: str) -> Dict:
    return json.loads(obj_str)

def msg_serialize(message: Message) -> str:
    return json.dumps(message, default=Message.to_json)

def msg_deserialize(json_str: str) -> Message:
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

def open_server_connection(ip, port) -> socket:
    try:
        sk = socket(AF_INET, SOCK_STREAM)
        sk.connect((ip, port))
        return sk
    except ConnectionRefusedError:
        print('Servidor não aceitou a conexão')

def close_server_connection(socket: socket) -> None:
    if socket is not None:
        socket.close()

# envia um comando sem esperar pela resposta
def send_and_forget(socket: socket, message: Message) -> None:
    cmd_str = msg_serialize(message)
    socket.sendall(cmd_str.encode())

# envia um comando e aguarda a resposta
def send_request(socket: socket, message: Message) -> Message:
    send_and_forget(socket, message)
    response = socket_receive_all(socket)
    return msg_deserialize(response)
