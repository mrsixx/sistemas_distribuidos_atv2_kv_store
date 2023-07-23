import os
import re
import helpers
from random import randint
from message import Message
from typing import Dict, List, Tuple
from threading import Thread
from dataclasses import dataclass
from socket import socket, AF_INET, SOCK_STREAM

@dataclass
class Client:
    def __init__(self) -> None:
        self._servers_adresses  = []
   
    # region getters
    @property
    def servers_adresses(self) -> List[Tuple[str, int]]:
        return self._servers_adresses
    # endregion

    # inclui um server socket na lista de sockets disponíveis
    def set_server_address(self, server_address: str) -> None:
        ip, port = server_address.split(':')
        self.servers_adresses.append((ip, int(port)))
    
    def get_random_server_address(self) -> Tuple[str, int]:
        if not any(self.servers_adresses):
            raise Exception('Nenhum endereço de servidor disponível')
        
        idx = randint(a=0, b=len(self.servers_adresses)-1)
        return self.servers_adresses[idx]

    # region funções de comunicação com o servidor
    def open_server_connection(self) -> socket:
        ip, port = self.get_random_server_address()
        return helpers.open_server_connection(ip, port)

    def close_server_connection(self, socket: socket) -> None:
        helpers.close_server_connection(socket)
    # endregion

    # region features
    def init(self, addresses: List[str]) -> None:
        for address in addresses:
            self.set_server_address(address)
        
    def put(self, key: str, value: str) -> None:
        # abre-se uma conexão com o servidor
        conn = self.open_server_connection()
        try:
            if conn is not None:
                msg = self.put_command_factory(key, value)
                response = helpers.send_request(conn, msg)
                self.put_ok_command_handler(response)
        finally:
            self.close_server_connection(conn)

    def get(self, key: str) -> None:
         # abre-se uma conexão com o servidor
        conn = self.open_server_connection()
        try:
            if conn is not None:
                msg = self.get_command_factory(key)
                response = helpers.send_request(conn, msg)
                self.get_response_command_handler(response)
        finally:
            self.close_server_connection(conn)
    # endregion

    # region factories
    def put_command_factory(self, key:str, value: str) -> Message:
        cmd = Message('PUT').set_key(key).set_value(value)
        return cmd

    def get_command_factory(self, key: str) -> Message:
        #TODO obter timestamp real da chave
        cmd = Message('GET').set_key(key).set_client_timestamp(0)
        return cmd
    # endregion
    
    # region command handlers
    # handler responsavel por tratar confirmações de um PUT
    def put_ok_command_handler(self, put_ok_cmd: Message) -> None:
        key, value, timestamp, server_address = put_ok_cmd.key, put_ok_cmd.value, put_ok_cmd.server_timestamp, put_ok_cmd.sender_address
        print(f'PUT_OK key: {key} value {value} timestamp {timestamp} realizada no servidor {server_address}')
        

    # handler responsavel por tratar resultados de um GET
    def get_response_command_handler(self, get_response_cmd: Message) -> None:
        response_type = get_response_cmd.type
        if response_type == 'GET_OK':
            key, value, client_timestamp = get_response_cmd.key, get_response_cmd.value, get_response_cmd.client_timestamp
            server_timestamp, server_address = get_response_cmd.server_timestamp, get_response_cmd.sender_address
            print(f'GET key: {key} value: {value} obtido do servidor {server_address}, meu timestamp {client_timestamp} e do servidor {server_timestamp}')

    # endregion
    
    def run_iteractive_menu(self) -> None:
        handler_thread = self.IteractiveMenuThread(self)
        handler_thread.start()


    # classe aninhada para executar o menu iterativo
    class IteractiveMenuThread(Thread):
        def __init__(self, client) -> None:
            Thread.__init__(self)
            self._client = client

        # region getters
        @property
        def client(self):
            return self._client
        
        # endregion

        # override na função run da thread para execução do menu iterativo
        def run(self):
            print('KV Store -- Digite HELP caso precise de ajuda.')
            while True:
                try:
                    cli_input = input('\n>>> ').split(' ')
                    main_cmd = cli_input[0].upper()
                    args = cli_input[1:]

                    if main_cmd == 'INIT':
                        if len(args) < 1:
                            raise Exception('INIT espera por pelo menos um parâmetro `ip:porta`.\n')
                        for address in args:
                            if not re.match('^(?:(?:\d{1,3}\.){3}\d{1,3}|localhost):\d{1,5}$', address):
                                raise Exception(f'{address} não é um endereço válido.\n')
                        self.client.init(addresses=args)
                    elif main_cmd == 'PUT':
                        if len(args) != 2:
                            raise Exception('PUT espera pelos parâmetros `key` e `value`.\n')
                        key, value = args[0], args[1]
                        self.client.put(key, value)
                    elif main_cmd == 'GET':
                        if len(args) != 1:
                            raise Exception('GET espera pelo parâmetro `key`.\n')
                        self.client.get(key=args[0])
                    elif main_cmd == 'EXIT':
                        raise KeyboardInterrupt()
                    elif main_cmd == 'HELP':
                        print('Os comandos disponíveis são:\n')
                        print('INIT ip:porta [, ip:porta]*: Configura os endereços dos servidores.\n')
                        print('PUT key value: Envia o par <key,value> para o servidor.\n')
                        print('GET key: Solicita ao servidor pelo valor correspondente a chave `key`.\n')
                        print('EXIT: Encerra a execução.\n')
                    else:
                        pass
                except (KeyboardInterrupt, EOFError):
                    print('\nSaindo...')
                    break
                except Exception as e:
                    print('Erro:', e)
                    print('\n')
                    pass
            os._exit(os.EX_OK) 

def main():
    try:
        # instancio o client
        client = Client()
        # coloco a command line interface do client para rodar
        client.run_iteractive_menu()
    except ValueError as error:
        print(error)

if __name__ == '__main__':
    main()