import paho.mqtt.client as mqtt
from random import randrange
import sys
import numpy as np
from time import sleep

total_nodes = 8       # Total de nós na DHT
nodes = np.array([])  # Para guardar os nodeIDs
ack_nodes = np.array([])  # Para guardar os nós prontos


def check_interval(k):
    k = int(k)
    if index == 0:  # Se é o primeiro nó do intervalo, seu antecessor possui nodeID menor que o seu
        return (nodes[ant] < k <= rangeAddr) or (0 <= k <= nodes[index])

    return nodes[ant] < k <= nodes[index]  # Checa se esta dentro do intervalo de responsabilidade


def on_message(client, userdata, msg):
    topic = msg.topic
    m = msg.payload.decode("utf-8")

    if topic == "join":
        global nodes
        m = int(m)  # m é um nodeID, converte pra int
        nodes = np.unique(np.append(nodes, m))  # insere no array de nós, o unique retira repetidos

    elif topic == "ack-join":
        global ack_nodes
        m = int(m)  # m é um nodeID, converte pra int
        ack_nodes = np.unique(np.append(ack_nodes, m))  # o nó em questao que fez o ack esta pronto

    elif topic == "put":
        key, value = m.split(" ", 1)  # m é uma mensagem no formato "chave string"
        if check_interval(key):
            key = int(key)
            hashTable[key] = value
            client.publish("ack-put", str(nodeID))
            print("Valor %s armazenado com sucesso na chave %s." % (value, str(key)))
        
    else:  # get
        if check_interval(m):  # Recebe uma chave
            key = int(m)
            value = hashTable[key]
            client.publish("res-get", value)
            print("Valor %s retornado da chave %s." % (value, str(key)))


rangeAddr = 2 ** 32  # Quantidade máxima de endereços na tabela hash
hashTable = {}
nodeID = randrange(0, rangeAddr)

mqttBroker = "127.0.0.1"  # Broker tem IP local e porta padrão

# Verifica se foi passado um nome/número específico para o node
if len(sys.argv) > 1:
    name = sys.argv[1]
else:
    name = nodeID

# Conecta ao broker mqtt
client = mqtt.Client("Node_%s" % name)  # Passar como parâmetro o nome/número do nó
while client.connect(mqttBroker) != 0:
    sleep(0.1)
print("Node_%s conectado ao broker." % name, "ID: " + str(nodeID))

# Se inscreve para receber os outros 7 nodeID's
client.subscribe("join")
client.subscribe("ack-join")
client.on_message = on_message
client.loop_start()

print("Preparando para receber mensagens...")
# Publica seu nodeID no topic "join"
while nodes.size < total_nodes or ack_nodes.size < total_nodes:  # Enquanto não está pronto ou os outros nós não estão
    client.publish("join", nodeID)

    if nodes.size == total_nodes:  # Nó está pronto, logo avisa aos outros
        client.publish("ack-join", nodeID)

    sleep(0.5)  # Espera um tempo

# Obtem antecessor e sucessor
nodes.sort()
index = np.where(nodes == nodeID)[0][0]
ant, suc = (index - 1, index + 1)  # antecessor e sucessor
if suc == total_nodes:
    suc = 0

print("Pronto! Intervalo de responsabilidade: (", int(nodes[ant]), ",", int(nodes[index]), "]")
# Se inscreve no tópico put e get
client.subscribe("put")
client.subscribe("get")

while True: 
    continue
