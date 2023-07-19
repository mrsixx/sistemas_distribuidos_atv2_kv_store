import os
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

# def download_file(path: str, file_name: str, file_size: int, socket) -> Dict:
#     try:
#         mb = 1024
#         buffer_size = 10 * mb
#         # recebo do peer provider o tamanho do arquivo
#         server_file_size = file_size
#         local_file_path = f'{path}/{file_name}'
#         if not os.path.isdir(path):
#             raise Exception('Diretorio destino nao existe')

#         # crio uma barra de progresso para o download
#         bar = tqdm(range(server_file_size), f"Baixando {file_name}", unit="B", unit_scale=True, unit_divisor=mb)
#         # crio o arquivo com permissão de escrita
#         with open(local_file_path, 'wb') as file:
#             while True:
#                 # enquanto houver, recebo um pacote e escrevo no arquivo
#                 temp = socket.recv(buffer_size)
#                 # se não houver mais conteudo, o download acabou
#                 if not temp:
#                     break
#                 file.write(temp)
#                 bar.update(len(temp))
#         # o download foi bem sucedido se o arquivo criado tiver o mesmo tamanho do original
#         local_file_size = os.path.getsize(local_file_path)
#         return { 'success': local_file_size == server_file_size, 'message': '' }
#     except Exception as e:
#         return { 'success': False, 'message': e }

# def list_path_files(path: str) -> List[str]:
#     files = []
#     for _, _, arquivos in os.walk(path):
#         for arquivo in arquivos:
#             files.append(arquivo)
#     return files