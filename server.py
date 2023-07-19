import helpers
from message import Message
from socket import socket, AF_INET, SOCK_STREAM
from dataclasses import dataclass
from threading import Thread, Lock
from typing import Dict, List

@dataclass
class Server:
    # construtor da classe server que recebe a parametrizacao do endereço ip:porta vinculado a instancia em execução
    def __init__(self, ip: str, port: int, ip_leader: str, port_leader) -> None:
        self._ip = ip
        self._port = port
        self.ip_leader = ip_leader
        self.port_leader = port_leader
        # criação do socket de conexão e bind no ip:porta parametrizado
        self._server_socket = socket(AF_INET, SOCK_STREAM)
        self._server_socket.bind((self.ip, self.port))
        self._server_socket.listen(5)
        # # estrutura de dados para armazenamento dos usuarios registrados
        self._store = dict()
        self._lock = Lock()

    #region getters
    @property
    def ip(self) -> str:
        return self._ip

    @property
    def port(self) -> int:
        return self._port

    @property
    def server_socket(self) -> socket:
        return self._server_socket

    @property
    def is_leader(self) -> bool:
        return self.ip == self.ip_leader and self.port == self.port_leader
    #endregion

    # region factories
    def put_ok_command_factory(self, key:str, value: str, server_timestamp: int) -> Message:
        put_ok_cmd = Message('PUT_OK').set_key(key).set_value(value).set_server_timestamp(server_timestamp)
        return put_ok_cmd

    def get_ok_command_factory(self, key:str, value: str, client_timestamp: int, server_timestamp: int) -> Message:
        get_ok_cmd = Message('GET_OK').set_key(key).set_value(value)
        get_ok_cmd.set_client_timestamp(client_timestamp).set_server_timestamp(server_timestamp)
        return get_ok_cmd
    # endregion

    # region command handlers
    def server_handle(self, command: Message) -> Message:
        cmd_name = command.type
        if cmd_name == 'PUT':
            return self.put_command_handler(command)
        if cmd_name == 'GET':
            return self.get_command_handler(command)
        return None

    def put_command_handler(self, put_cmd: Message) -> Message:
        key, value, client_timestamp, client_address = put_cmd.key, put_cmd.value, put_cmd.client_timestamp, put_cmd.sender_address
        server_timestamp = self.store_key_value_pair(key, value, client_timestamp)
        print(f'Cliente {client_address} PUT key:{key} value:{value}')
        return self.put_ok_command_factory(key, value, server_timestamp)

    def get_command_handler(self, get_cmd: Message) -> Message:
        key, client_timestamp, client_address = get_cmd.key, get_cmd.client_timestamp, get_cmd.sender_address
        stored = self.get_key_value_pair(key)
        value, server_timestamp = stored['value'], stored['timestamp']
        # TODO tratar erro e incluir na mensagem "portanto devolvendo [value ou erro]"
        print(f'Cliente {client_address} GET key:{key} ts:{client_timestamp}. Meu ts é {server_timestamp}, portanto devolvendo {value}')
        return self.get_ok_command_factory(key, value, client_timestamp, server_timestamp)
    # endregion

    def close(self) -> None:
        self.server_socket.close()

    def listen(self) -> None:
        while True:
            try:
                # aguardo um client se conectar
                client_socket, client_address = self.server_socket.accept()
                # despacho para um thread tratar sua requisição
                handler_thread = self.RequestHandlerThread(self, client_socket, client_address)
                handler_thread.start()
            except (KeyboardInterrupt, EOFError):
                print('\nSaindo...')
                break
            except:
                pass

    def get_key_value_pair(self, key: str) -> Dict:
        formatted_key = key.upper()
        with self._lock:
            return self._store.get(formatted_key)

    # registra um client provedor de arquivos em um dicionario tridimensional para otimizar a busca
    def store_key_value_pair(self, key: str, value: str, timestamp: int) -> int:
        formatted_key = key.upper()
        with self._lock:
            # TODO validar inserção
            if formatted_key not in self._store:
                self._store[formatted_key] = dict([('value', ''),('timestamp', 0)])
            new_timestamp = self._store.get(formatted_key)['timestamp'] + 1
            self._store[formatted_key]['value'] = value
            self._store[formatted_key]['timestamp'] = new_timestamp
            return new_timestamp

    # classe aninhada para fazermos o dispatch da requisição para outras threads
    class RequestHandlerThread(Thread):
        def __init__(self, server, client_socket, client_address) -> None:
            Thread.__init__(self)
            self._server = server
            self._client_socket = client_socket
            self._client_address = client_address
      
        # region getters
        @property
        def server(self):
            return self._server
        
        @property
        def client_socket(self) -> socket:
            return self._client_socket
        
        @property
        def client_address(self):
            return self._client_address
        # endregion

        # sobrescrevendo a função run
        def run(self):
            request = helpers.socket_receive_all(self.client_socket)
            if self.server.is_leader:
                response_cmd = self.process_request(request)
                if response_cmd is not None:
                    self.client_socket.sendall(self.prepare_response(response_cmd))
            # TODO: redirect request
            self.client_socket.close()

        # aciona o command handler para dar o tratamento adequado de acordo com o comando recebido
        def process_request(self, request: bytes) -> Message:
            command = helpers.json_deserialize(request.decode())
            # incluo na mensagem os dados do remetente
            command.set_sender(ip=self.client_address[0], port=self.client_address[1])
            # chamo o service locator que encaminhará a mensagem para ser tratada pelo handler adequado
            return self.server.server_handle(command)
        
        # monta o fluxo de bytes que será encaminhado como response à request recebida
        def prepare_response(self, response_cmd: Message) -> bytes:
            # incluo na resposta os dados do remetente
            response_cmd.set_sender(self.server.ip, self.server.port)
            return helpers.json_serialize(response_cmd).encode()     

def main():
    try:
        ip = input('IP (default 127.0.0.1): ') or '127.0.0.1'
        port = int(input('Port (default 1099): ') or '1099')
        ip_leader = input('IP líder (default 127.0.0.1): ') or '127.0.0.1'
        port_leader = int(input('Port líder (default 1099): ') or '1099')
        server = Server(ip, port, ip_leader, port_leader)
        try:
            server.listen()
        finally:
            server.close()
    except Exception as e:
        print('Erro durante a execução: ', e)

if __name__ == '__main__':
    main()