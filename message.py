from typing import Dict
from dataclasses import dataclass

@dataclass
class Message:
    def __init__(self, type: str) -> None:
        self._type  = type
        self._key   = ''
        self._value = ''
    
    @property
    def type(self) -> str:
        return self._type
    
    @property
    def key(self) -> str:
        return self._key
    
    @property
    def value(self) -> str:
        return self._value

    def set_key(self, key: str) -> None:
        self._key = key
        
    def set_value(self, value: str) -> None:
        self._value = value
    
    @staticmethod
    def to_json(msg) -> Dict:
        if isinstance(msg, Message):
            decode_array = [(key, msg.__dict__[key]) for key in msg.__dict__]
            decode_array.append(('__class__', Message.__name__))
            decode_array.append(('__module__', Message.__module__))
            return dict(decode_array)
        raise TypeError(f"Objeto do tipo '{msg.__class__.__name__}' não JSON-serializável")

    @staticmethod
    def from_json(d: Dict):
        if d['__class__'] == Message.__name__:
            inst = Message(d['_type'])
            inst.set_key(d['_key'])
            inst.set_value(d['_value'])
        else:
            inst = d
        return inst