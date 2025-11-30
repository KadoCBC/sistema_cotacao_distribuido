import socket
import json

HOST = "localhost"
PORT = 1100

def main():
    topico = input("Digite o tópico para se inscrever: ")
    try:
        with socket.create_connection((HOST, PORT)) as s:
            dados = json.dumps({
                "tipo": "sub",
                "topico": topico
            }) + "\n"
            s.sendall(dados.encode('utf-8'))
            print(f"Inscrito no tópico '{topico}'. Aguardando mensagens...\n")

            buffer = ""
            while True:
                dados_recebidos = s.recv(1024)
                if not dados_recebidos:
                    break
                
                buffer += dados_recebidos.decode('utf-8')
                
                while '\n' in buffer:
                    linha, buffer = buffer.split('\n', 1)
                    linha = linha.strip()
                    if not linha:
                        continue
                    
                    print(linha)

    except Exception as e:
        print(f"Erro na conexão com o broker: {e}")

if __name__ == "__main__":
    main()