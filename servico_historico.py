import socket
import json
import multiprocessing 
import os
import time

# Configurações
HOST = "localhost"
PORTA_ESCRITA = 1300      # Nova porta para receber writes do servico_cotacao
PORTA_CONSULTA = 1200    # Porta para RESPONDER dados (Read)
NUM_SHARDS = 3

# determina a shard com base no nome do ativo
def determinar_shard(ativo_nome):
    soma = sum(ord(c) for c in ativo_nome)
    return soma % NUM_SHARDS

# Função para buscar últimas transações de um ativo -> VERIFICAR ISSO DEPOIS
def buscar_ultimas_transacoes(ativo, limite=10):

    # Determina a shard
    shard_id = determinar_shard(ativo)
    #Le arquivo correspondente
    nome_arquivo = f"historico_shard_{shard_id}.txt"
    transacoes = []
    
    if not os.path.exists(nome_arquivo):
        return []

    try:
        with open(nome_arquivo, "r") as f: 
            linhas = f.readlines()
            for linha in reversed(linhas):
                partes = linha.strip().split("|")
                if len(partes) >= 2:
                    if partes[0] == ativo:
                        transacoes.append(float(partes[1]))
                        if len(transacoes) >= limite:
                            break
    except Exception as e:
        print(f"[ERRO LEITURA] {e}")

    return transacoes

def salvar_no_historico(ativo, preco):

    ativo = ativo #sim, está redundante
    preco = preco #sim, está redundante
    
    shard_id = determinar_shard(ativo)
    nome_arquivo = f"historico_shard_{shard_id}.txt"
    linha = f"{ativo}|{preco}\n"
    
    try:
        # 'a' = append (adicionar ao final)
        with open(nome_arquivo, "a") as arquivo:
            arquivo.write(linha)
        print(f"[GRAVACAO] {ativo} -> Shard {shard_id}")
    except Exception as e:
        print(f"[ERRO GRAVACAO] {e}")

# SOCKET SERVER PARA CONSULTAS (READ)
def processo_servidor_consulta():
    print(f"--> [Processo {os.getpid()}] Iniciando Servidor de Consulta na porta {PORTA_CONSULTA}...")
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # oque é isso?
    
    try:
        server.bind((HOST, PORTA_CONSULTA))
        server.listen()
        
        while True:
            # Aceita conexão
            conexao, endereco_cliente = server.accept()
            
            # Truque do makefile para facilitar leitura/escrita
            canal = conexao.makefile('rw')
            
            try:
                # Lê apenas a primeira linha do pedido
                linha = canal.readline()
                if linha:
                    req = json.loads(linha)
                    ativo = req.get("ativo")
                    
                    if ativo:
                        historico = buscar_ultimas_transacoes(ativo)
                        resposta = json.dumps({"ativo": ativo, "historico": historico}) + "\n"
                        canal.write(resposta)
                        canal.flush()
            except Exception as e:
                print(f"[ERRO CONSULTA] {e}")
            finally:
                conexao.close()
                
    except Exception as e:
        print(f"Erro fatal no processo de consulta: {e}")

# SOCKET CLIENT PARA CONSUMIR DADOS DO SERVIÇO DE COTACAO
def processo_servidor_escrita():
    print(f"--> [Processo {os.getpid()}] Iniciando Servidor de Escrita na porta {PORTA_ESCRITA}...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind((HOST, PORTA_ESCRITA))
        server.listen()
        while True:
            conn, addr = server.accept()
            canal = conn.makefile('rw')
            try:
                linha = canal.readline()
                if linha:
                    try:
                        req = json.loads(linha)
                        ativo = req.get("ativo")
                        preco = req.get("preco")
                        if ativo and (preco is not None):
                            # aceita preco como número ou string convertível
                            try:
                                preco_f = float(preco)
                                salvar_no_historico(ativo, preco_f)
                            except:
                                print("[ESCRITA] preco inválido:", preco)
                    except json.JSONDecodeError:
                        print("[ESCRITA] JSON inválido recebido")
            except Exception as e:
                print(f"[ERRO ESCRITA] {e}")
            finally:
                conn.close()
    except Exception as e:
        print(f"[ERRO FATAL ESCRITA] {e}")
    finally:
        server.close()

# --- MAIN ---
if __name__ == "__main__":
    
    # Processo A
    p_consulta = multiprocessing.Process(target=processo_servidor_consulta)
    
    # Processo B
    p_escrita = multiprocessing.Process(target=processo_servidor_escrita)

    # Daemon=True significa: se o programa principal fechar, mata os filhos
    p_consulta.daemon = True
    p_escrita.daemon = True

    p_consulta.start()
    p_escrita.start()

    print(f"Sistema de Histórico (Sharding) Ativo.")
    print(f"PID Principal: {os.getpid()}")

    # Mantém o processo pai vivo para os filhos não morrerem
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nEncerrando sistema...")