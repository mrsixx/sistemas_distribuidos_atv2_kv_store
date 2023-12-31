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
        # criação do server socket e bind no ip:porta parametrizado
        self._server_socket = socket(AF_INET, SOCK_STREAM)
        self._server_socket.bind((self.ip, self.port))
        self._server_socket.listen(5)
        # estrutura de dados para armazenamento dos pares chave-valor registrados
        self._store = dict()
        # lock para gerenciar alterações concorrentes
        self._lock = Lock()
        # lista que registra os followers
        self._followers = []

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
    
    @property
    def store(self) -> Dict:
        return self._store
    
    @property
    def followers(self) -> List:
        return self._followers
    #endregion

    # region setters
    def add_follower(self, ip: str, port: int) -> None:
        self._followers.append((ip, port))
    
    def set_store(self, store: Dict) -> None:
        self._store = store
    # endregion

    # abre conexão com um servidor follower para fins de replicação
    def open_follower_conection(self, ip: str, port: int) -> socket:
        return helpers.open_server_connection(ip, port)

    # abre conexão com o servidor líder para encaminhar uma requisição PUT
    def open_leader_connection(self) -> socket:
        return helpers.open_server_connection(self.ip_leader, self.port_leader)

    # fecha a conexão com um servidor
    def close_server_connection(self, socket: socket) -> None:
        helpers.close_server_connection(socket)

    # region factories
    # Monta um PUT_OK command, carregando a chave, valor e o timestamp incrementado pelo servidor
    def put_ok_command_factory(self, key:str, value: str, server_timestamp: int) -> Message:
        put_ok_cmd = Message('PUT_OK').set_key(key).set_value(value).set_server_timestamp(server_timestamp)
        return put_ok_cmd

    # Monta um GET_OK Command, carregando chave, valor e os timestamps do cliente e o do servidor
    def get_ok_command_factory(self, key:str, value: str, client_timestamp: int, server_timestamp: int) -> Message:
        get_ok_cmd = Message('GET_OK').set_key(key).set_value(value)
        get_ok_cmd.set_client_timestamp(client_timestamp).set_server_timestamp(server_timestamp)
        return get_ok_cmd
    
    # Monta um erro TRY_OTHER_SERVER_OR_LATER carregando a chave responsável pelo erro
    def try_another_command_factory(self, key:str) -> Message:
        return Message('TRY_OTHER_SERVER_OR_LATER').set_key(key)

    # Monta um FOLLOW command carregando o endereço do servidor que deseja se juntar a rede
    def follow_command_factory(self, ip: str, port: int) -> Message:
        follow_cmd = Message('FOLLOW').set_follower_address(ip, port)
        return follow_cmd

    # Monta um FOLLOW_OK command, carregando uma cópia do estado atual do servidor líder para manter a consistência
    def follow_ok_command_factory(self) -> Message:
        follow_ok_cmd = Message('FOLLOW_OK').set_store_json(helpers.json_serialize(self.store))
        return follow_ok_cmd
    
    # Monta um REPLICATION command, carregando chave, valor e o timestamp incrementado pelo líder
    def replication_commmand_factory(self, key:str, value: str, leader_timestamp: int) -> Message:
        replication_cmd = Message('REPLICATION').set_key(key).set_value(value).set_server_timestamp(leader_timestamp)
        return replication_cmd
    
    # Monta um REPLICATION_OK command, para informar o servidor do sucesso da replicação
    def replication_ok_command_factory(self) -> Message:
        return Message('REPLICATION_OK')
    # endregion

    # region command handlers
    def server_handle(self, command: Message) -> Message:
        cmd_name = command.type
        if cmd_name == 'PUT':
            return self.put_command_handler(command)
        if cmd_name == 'GET':
            return self.get_command_handler(command)
        if cmd_name == 'FOLLOW':
            return self.follow_command_handler(command)
        if cmd_name == 'FOLLOW_OK':
            return self.follow_ok_command_handler(command)
        if cmd_name == 'REPLICATION':
            return self.replication_command_handler(command)
        return None

    # inclui/atualiza o valor de uma chave
    def put_command_handler(self, put_cmd: Message) -> Message:
        key, value = put_cmd.key, put_cmd.value
        if self.is_leader:
            client_timestamp, client_address = put_cmd.client_timestamp, put_cmd.sender_address
            server_timestamp = self.store_key_value_pair(key, value, client_timestamp)
            print(f'Cliente {client_address} PUT key:{key} value:{value}')
            for (ip, port) in self.followers:
                replication_cmd = self.replication_commmand_factory(key, value, server_timestamp)
                self.replicate(replication_cmd, ip, port)
            
            print(f'Enviando PUT_OK ao Cliente {client_address} da key:{key} ts:{server_timestamp}.')
            return self.put_ok_command_factory(key, value, server_timestamp)
        
        print(f'Encaminhando PUT key:{key} value:{value}')
        return self.send_put_to_leader(put_cmd)

    # devolve o conteudo de uma chave
    def get_command_handler(self, get_cmd: Message) -> Message:
        key, client_timestamp, client_address = get_cmd.key, get_cmd.client_timestamp, get_cmd.sender_address
        stored = self.get_key_value_pair(key)
        value, server_timestamp = stored['value'], stored['timestamp']

        if client_timestamp > server_timestamp:
            print(f'Cliente {client_address} GET key:{key} ts:{client_timestamp}. Meu ts é {server_timestamp}, portanto devolvendo TRY_OTHER_SERVER_OR_LATER')
            return self.try_another_command_factory(key)
            
        value = 'NULL' if value is None else value
        print(f'Cliente {client_address} GET key:{key} ts:{client_timestamp}. Meu ts é {server_timestamp}, portanto devolvendo {value}')
        return self.get_ok_command_factory(key, value, client_timestamp, server_timestamp)
    
    # inclui um servidor follower na lista
    def follow_command_handler(self, follow_cmd) -> None:
        ip, port = follow_cmd.follower_address
        self.add_follower(ip, port)
        return self.follow_ok_command_factory()

    # prepara a store do servidor que entrou na rede
    def follow_ok_command_handler(self, follow_ok_cmd: Message) -> None:
        store = helpers.json_deserialize(follow_ok_cmd.store_json)
        self.set_store(store)

    # replica uma chave
    def replication_command_handler(self, replication_cmd: Message) -> Message:
        key, value, timestamp = replication_cmd.key, replication_cmd.value, replication_cmd.server_timestamp
        self.store_key_value_pair(key, value, timestamp)
        print(f'REPLICATION key:{key} value:{value} ts:{timestamp}')
        return self.replication_ok_command_factory()
    # endregion

    # fecha uma conexão
    def close(self) -> None:
        self.server_socket.close()

    # Realiza o setup inicial do servidor, seguindo o líder caso seja um follower
    def setup(self) -> None:    
        if not self.is_leader:
            self.follow_leader()
    
    # repassa um PUT command recebido para o líder e retransmite ao cliente solicitante a resposta
    def send_put_to_leader(self, put_cmd: Message) -> Message:
        # abre-se uma conexão com o servidor
        conn = self.open_leader_connection()
        try:
            if conn is not None:
                return helpers.send_request(conn, put_cmd)
        finally:
            self.close_server_connection(conn)

    # Envia o replication_cmd para um servidor endereçado em ip:port
    def replicate(self, replication_cmd: Message, ip: str, port: int) -> None:
        conn = self.open_follower_conection(ip, port)
        try:
            if conn is not None:
                if helpers.send_request(conn, replication_cmd) is None:
                    raise Exception('Erro ao replicar.')
        finally:
            self.close_server_connection(conn)
    
    # recebe uma requisição e dispacha para uma thread dedicada ao tratamento
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
                break
                    
    # envia uma notificação para o líder avisando que se juntou a rede e recebe como resposta os dados que o servidor possui
    def follow_leader(self) -> None:
         # abre-se uma conexão com o servidor
        conn = self.open_leader_connection()
        try:
            if conn is not None:
                msg = self.follow_command_factory(self.ip, self.port)
                response = helpers.send_request(conn, msg)
                self.follow_ok_command_handler(response)
        finally:
            self.close_server_connection(conn)
    
    # obtem um par <chave, valor> a partir da chave
    def get_key_value_pair(self, key: str) -> Dict:
        formatted_key = key.upper()
        with self._lock:
            value = self._store.get(formatted_key)
            if value is not None:
                return value
            return dict([('value', 'NULL'),('timestamp', 0)])
            

    # registra um par <chave, valor>
    def store_key_value_pair(self, key: str, value: str, timestamp: int) -> int:
        formatted_key = key.upper()
        with self._lock:
            if formatted_key not in self._store:
                self._store[formatted_key] = dict([('value', 'NULL'),('timestamp', 0)])
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
            response_cmd = self.process_request(request)
            if response_cmd is not None:
                self.client_socket.sendall(self.prepare_response(response_cmd))
            self.client_socket.close()

        # aciona o command handler para dar o tratamento adequado de acordo com o comando recebido
        def process_request(self, request: bytes) -> Message:
            command = helpers.msg_deserialize(request.decode())
            # incluo os dados do remetente na mensagem
            if command is not None:
                command.set_sender(ip=self.client_address[0], port=self.client_address[1])
            # chamo o service locator que encaminhará a mensagem para ser tratada pelo handler adequado
            return self.server.server_handle(command)
        
        # monta o fluxo de bytes que será encaminhado como response à request recebida
        def prepare_response(self, response_cmd: Message) -> bytes:
            # incluo na resposta os dados do remetente
            response_cmd.set_sender(self.server.ip, self.server.port)
            return helpers.msg_serialize(response_cmd).encode()     

def main():
    try:
        ip = input('IP: ') or '127.0.0.1'
        port = int(input('Port: '))
        ip_leader = input('Leader IP: ') or '127.0.0.1'
        port_leader = int(input('Leader Port: '))
        server = Server(ip, port, ip_leader, port_leader)
        try:
            server.setup()
            server.listen()
        finally:
            server.close()
    except Exception as e:
        print('Erro durante a execução: ', e)

if __name__ == '__main__':
    main()