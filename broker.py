import socket
import threading
import json

# Configurações
HOST = 'localhost'
PORT = 1100

# Variáveis Globais
topicos = {} 
lock = threading.Lock()

def thread_gerenciar_cliente(conexao, endereco_cliente):
    print(f"[NOVA CONEXÃO] {endereco_cliente} conectado.")
    buffer = ""
    
    # Variáveis para controle de estado dessa thread
    topico_inscrito = None
    eh_inscrito = False

    try:
        while True:
            try:
                # Chunk de dados (1024 bytes)
                dados = conexao.recv(1024)
                if not dados:
                    break # Conexão encerrada pelo cliente (retornou vazio)
                
                msg_recebida = dados.decode('utf-8')
                buffer += msg_recebida
            except ConnectionResetError:
                break

            # Processa enquanto houver quebras de linha no buffer
            while '\n' in buffer:
                linha, buffer = buffer.split('\n', 1)
                linha = linha.strip()
                
                if not linha: continue

                try:
                    # Parse do JSON
                    dados_json = json.loads(linha)
                    tipo = dados_json.get("tipo")
                    topico = dados_json.get("topico")

                    # SUB
                    if tipo == "sub":
                        with lock: # Bloqueia para mexer no dict compartilhado
                            if topico not in topicos:
                                topicos[topico] = []
                            # Evita duplicidade
                            if conexao not in topicos[topico]:
                                topicos[topico].append(conexao)
                                
                        # Atualiza estado local para facilitar remoção depois
                        topico_inscrito = topico
                        eh_inscrito = True
                        print(f"Cliente {endereco_cliente} INSCRITO em {topico}")

                    # PUB
                    elif tipo == "pub":
                        msg = dados_json.get('mensagem', '')
                        print(f"Recebido PUB em {topico}: {msg}")
                        
                        with lock:
                            if topico in topicos:
                                # Prepara mensagem para repassar
                                msg_final = msg + "\n"
                                
                                # Copia a lista para iterar com segurança
                                lista_clientes = topicos[topico][:] 
                                
                                for sub in lista_clientes:
                                    try:
                                        sub.sendall(msg_final.encode('utf-8'))
                                        print(msg_final.encode('utf-8'))
                                    except:
                                        # Se der erro ao enviar, assume que caiu e remove
                                        print(f"Removendo cliente morto de {topico}")
                                        topicos[topico].remove(sub)
                            else:
                                print(f"Nenhum inscrito no tópico {topico}")

                except json.JSONDecodeError:
                    print("Erro: JSON inválido recebido")
                except Exception as e:
                    print(f"Erro processando mensagem: {e}")

    finally:
        # Lógica de limpeza (Desconectar)
        if eh_inscrito and topico_inscrito:
            with lock:
                if topico_inscrito in topicos and conexao in topicos[topico_inscrito]:
                    topicos[topico_inscrito].remove(conexao)
                    print(f"Cliente {endereco_cliente} removido de {topico_inscrito}")
        
        conexao.close()
        print(f"Conexão com {endereco_cliente} encerrada.")

if __name__ == "__main__":
    # Cria socket TCP
    socket_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Permite reusar a porta imediatamente se fechar o servidor e abrir de novo
    socket_tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    socket_tcp.bind((HOST, PORT))
    socket_tcp.listen(5) 

    print(f"Broker (Threading) rodando em {HOST}:{PORT}")
    print("Aguardando conexões...")

    try:
        while True:
            conexao, endereco_cliente = socket_tcp.accept()
            
            # Cria e inicia a Thread
            thread = threading.Thread(target=thread_gerenciar_cliente, args=(conexao, endereco_cliente))
            thread.start()
            
            print(f"Total de Threads ativas: {threading.active_count() - 1}")
            
    except KeyboardInterrupt:
        print("\nEncerrando broker...")
    finally:
        socket_tcp.close()
