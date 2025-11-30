import socket
import json
import time

HOST = "localhost"
PORT = 1100

s = socket.create_connection((HOST, PORT))

try:
    # Envia solicitação para publicar no tópico
    dados = json.dumps({
        "tipo": "pub",
        "topico": "noticias",
        "mensagem": "teste"
    }) + "\n"
    s.sendall(dados.encode('utf-8'))
    print("Conectado como publisher no tópico 'noticias'\n")

    
    close = json.dumps({
        "tipo": "pub",
        "topico": "noticias",
        "mensagem": ""
    }) + "\n"
    s.sendall(close.encode('utf-8'))
    print("Enviei mensagem de encerramento para o broker")
    
finally:
    print("Encerrando publisher")
    s.close()