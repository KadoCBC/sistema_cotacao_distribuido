import socket
import json
import time
import random

HOST = "localhost"
PORT = 1100 # Broker
PORT_HISTORICO = 1300 # Servico Historico


# Classe que implementa a 
class circuitBreaker:

    def __init__(self):
        self.estado = "FECHADO" # Estado inicial do circuito
        self.contador_falhas = 0
        self.limite_falhas = 2
        self.tempo_espera = 30  # segundos
        self.ultima_falha = 0 # timestamp da última falha
    
    def simular_chamada_api(self, ativo):
        if random.random() < 0.1:  # 10% de chance de falha
            raise Exception("Falha na API")
        
        TOPICOS_ATIVOS ={
            "maça": "frutas",
            "ouro": "metais",
            "laranja": "frutas" 
        }
        topico = TOPICOS_ATIVOS.get(ativo, "geral") #geral é default
        preco = 20 + random.random() #Simulação de resposta da API preco
        return  {"ativo": ativo, "preco": preco, "topico": topico }
    
    def chamar_api(self, ativo):

        if self.estado == "ABERTO":
            agora = time.time() # contagem de tempo atual
            if (agora - self.ultima_falha) > self.tempo_espera:
                print("Mudando para MEIO-ABERTO")
                self.estado = "MEIO-ABERTO"
            else:
                print("CIRCUITO ABERTO: Chamada bloqueada.")
                time.sleep(3) #espera um pouco antes de tentar de novo
                return None
        
        #Tentativa de conexão com a API
        try:
            resposta = self.simular_chamada_api(ativo) # se der certo codigo continua aqui

            if self.estado == "MEIO-ABERTO":
                print("Fechando circuito")
                self.estado = "FECHADO"
                self.contador_falhas = 0
            else:
                self.contador_falhas = 0

            return resposta
        
        # Se der erro na chamada
        except Exception as e:
            self.contador_falhas += 1
            print(f"Falha na chamada da API: {e}")
            self.ultima_falha = time.time() #guarda o tempo da falha

            if self.estado == "MEIO-ABERTO":
                self.estado = "ABERTO"
                print("CIRCUITO ABERTO: Falha no estado MEIO-ABERTO.")
                return None
            
            if self.contador_falhas >= self.limite_falhas:
                self.estado = "ABERTO"
                print("CIRCUITO ABERTO: Muitas falhas.")
            return None

def enviar_historico(ativo, preco):
    try:
        with socket.create_connection((HOST, PORT_HISTORICO)) as s:
            dados = json.dumps({
                "ativo": ativo,
                "preco": preco
            }) + "\n"
            s.sendall(dados.encode('utf-8'))
            print(f"Enviado para histórico: {ativo} -> {preco}")
    except Exception as e:
        print(f"Erro ao enviar para histórico: {e}")


def main():

    random.seed() # Inicializa gerador de números aleatórios
    disjuntor = circuitBreaker()

    # enquanto não temos bd
    ativos = ["maça", "banana", "ouro"]
    ultimas_cotacoes = [] #lista de dicionarios

    # Conecta ao broker
    try:
        s = socket.create_connection((HOST, PORT))
    except Exception as e:
        print(f"Erro ao conectar ao broker: {e}")
        return
    
    try:
        #loop para percurrer a lista de ativos
        while True:
            for ativo in ativos:
                
                retorno_api = disjuntor.chamar_api(ativo) #retorno da API

                if retorno_api != None:

                    preco = retorno_api.get("preco")

                    if len(ultimas_cotacoes) > 0:
                        ultimo_produto = ultimas_cotacoes[len(ultimas_cotacoes)-1]
                        preco_antigo = ultimo_produto.get("preco")
                    
                    preco_antigo = None 
                
                    if preco != preco_antigo:

                        ultimas_cotacoes.append(retorno_api)

                        ativo = retorno_api.get("ativo")
                        topico = retorno_api.get("topico")

                        # Envia para o broker
                        dados = json.dumps({
                            "tipo": "pub",
                            "topico": topico, 
                            "mensagem": f"ativo:{ativo} preco:{preco}"
                        }) + "\n"

                        s.sendall(dados.encode('utf-8'))

                        # Envia para o serviço de histórico
                        enviar_historico(ativo, preco)

                        print(f"Publicado como publisher no tópico: {topico}\n")
                        time.sleep(15)  # Aguarda 5 segundos antes da próxima cotação
    
    except KeyboardInterrupt:
        print("Encerrando publisher")

if __name__ == "__main__":
    main()