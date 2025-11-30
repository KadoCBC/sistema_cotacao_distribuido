import socket
import json
import time
import random

HOST = "localhost"
PORT = 1100

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
        return 20 + random.random()  #Simulação de resposta da API
    
    def chamar_api(self, ativo):

        if self.estado == "ABERTO":
            agora = time.time() # contagem de tempo atual
            if (agora - self.ultima_falha) > self.tempo_espera:
                print("Mudando para MEIO-ABERTO")
                self.estado = "MEIO-ABERTO"
            else:
                print("CIRCUITO ABERTO: Chamada bloqueada.")
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
          
def main():
    random.seed() # Inicializa gerador de números aleatórios

    disjuntor = circuitBreaker()

    # Conecta ao broker
    s = socket.create_connection((HOST, PORT))

    # enquanto não temos bd
    ativos = ["maça", "banana", "laranja"]

    ultimas_cotacoes = {}
    
    try:
        #loop para percurrer a lista de ativos (temporariomente)
        while True:
            for ativo in ativos:
                
                preco = disjuntor.chamar_api(ativo)

                preco_antigo = ultimas_cotacoes.get(ativo)

                if preco != preco_antigo and preco is not None:
                    ultimas_cotacoes[ativo] = preco

                    print(preco)
                    dados = json.dumps({
                        "tipo": "pub",
                        "topico": "noticias", #topico teste
                        "mensagem": f"preço:{preco}"
                    }) + "\n"

                    s.sendall(dados.encode('utf-8'))
                    print("Conectado como publisher no tópico 'noticias'\n")
                time.sleep(5)  # Aguarda 5 segundos antes da próxima cotação
    
    except KeyboardInterrupt:
        print("Encerrando publisher")

if __name__ == "__main__":
    main()