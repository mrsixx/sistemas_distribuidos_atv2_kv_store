from typing import Dict
from dataclasses import dataclass

@dataclass
class Message:
    def __init__(self, type: str) -> None:
        self._type  = type
        self._key   = ''
        self._value = ''
        self._client_timestamp = 0
        self._server_timestamp = 0
        self._sender = ('', 0)
    # region getters
    @property
    def type(self) -> str:
        return self._type
    
    @property
    def key(self) -> str:
        return self._key
    
    @property
    def value(self) -> str:
        return self._value

    @property
    def client_timestamp(self) -> int:
        return self._client_timestamp

    @property
    def server_timestamp(self) -> int:
        return self._server_timestamp

    @property
    def sender_address(self) -> str:
        ip, port = self._sender
        return f'{ip}:{port}'
    # endregion

    # region setters
    def set_key(self, key: str):
        self._key = key
        return self
        
    def set_value(self, value: str):
        self._value = value
        return self
    
    def set_client_timestamp(self, timestamp: int):
        self._client_timestamp = timestamp
        return self
    
    def set_server_timestamp(self, timestamp: int):
        self._server_timestamp = timestamp
        return self

    def set_sender(self, ip: str, port: int):
        self._sender = (ip, port)
        return self
    # endregion

    # region métodos estáticos para serialização/deserialização
    @staticmethod
    def to_json(msg) -> Dict:
        if isinstance(msg, Message):
            # usando list comprehension, mapeio propriedades e valores do dicionario em uma lista de tuplas
            decode_array = [(key, msg.__dict__[key]) for key in msg.__dict__]
            # incluo uma informação no json para validar a deserialização
            decode_array.append(('__class__', Message.__name__))
            # monto um dicionario generico
            return dict(decode_array)
        raise TypeError(f"Objeto do tipo '{msg.__class__.__name__}' não JSON-serializável")

    @staticmethod
    def from_json(d: Dict):
        if d['__class__'] == Message.__name__:
            msg = Message(d['_type'])
            msg.set_key(d['_key']).set_value(d['_value'])
            msg.set_client_timestamp(d['_client_timestamp']).set_server_timestamp(d['_server_timestamp'])
            msg.set_sender(d['_sender'][0],d['_sender'][1])
        else:
            msg = d
        return msg
    # endregion