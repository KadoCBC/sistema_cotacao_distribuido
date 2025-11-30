import socket
import json

HOST = "localhost"
PORT = 1100

s = socket.create_connection((HOST, PORT))

try:
    # Envia solicitação para se inscrever no tópico
    dados = json.dumps({
        "tipo": "sub",
        "topico": "noticias"
    }) + "\n"
    s.sendall(dados.encode('utf-8'))

    print("Inscrito no tópico 'noticias'. Aguardando mensagens...\n")

    # Fica aguardando mensagens do broker
    while True:
        msg = s.recv(1024).decode('utf-8')
        if msg:
            print(f"Mensagem recebida: {msg}")
finally:
    print("Encerrando subscriber")
    s.close()