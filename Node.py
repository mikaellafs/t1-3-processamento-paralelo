import paho.mqtt.client as mqtt
from random import randrange
import sys
import numpy as np
from time import sleep

total_nodes = 8       # total de nós na DHT
nodes = np.array([])  # para guardar os nodesId
ack_nodes = np.array([])  # para guardar os nós prontos


def check_interval(k):
    k = int(k)
    if index == 0:  # se é o primeiro nó do intervalo, seu antecessor possui nodeId menor que o seu
        return (nodes[ant] < k < rangeAddr) or (0 < k <= nodes[index])

    return nodes[ant] < k <= nodes[index]  # checa se esta dentro do intervalo de responsabilidade


def on_message(client, userdata, msg):
    topic = msg.topic
    m = msg.payload.decode("utf-8")

    if topic == "join":
        global nodes
        m = int(m)  # é um node id, converte pra int
        nodes = np.unique(np.append(nodes, m))  # insere no array de nós, o unique retira repetidos

    elif topic == "ack-join":
        global ack_nodes
        m = int(m)  # é um node id, converte pra int
        ack_nodes = np.unique(np.append(ack_nodes, m))  # o nó em questao que fez o ack esta pronto

    elif topic == "put":
        key, value = m.split(" ", 1)  # é uma mensagem com "chave string"
        if check_interval(key):
            hashTable[key] = value
            client.publish("ack-put", str(nodeID))
            print("Valor %s armazenado com sucesso na chave." % value, key)
        
    else:  # get
        if check_interval(m):  # recebe uma chave
            key = int(m)
            value = hashTable[key]
            client.publish("res-get", value)
            print("Valor %s retornado da chave %s." % (value, key))


rangeAddr = 2 ** 32
hashTable = {}
nodeID = randrange(0, rangeAddr)

mqttBroker = "127.0.0.1"

if len(sys.argv) >1:
    name = sys.argv[1]
else:
    name = nodeID

# Conecta ao broker mqtt
client = mqtt.Client("Node_%s" % name)  # passar como parametro o nome/numero do nó
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
while nodes.size < total_nodes or ack_nodes.size < total_nodes:  # enquanto nao esta pronto ou os outros nós nao estao
    client.publish("join", nodeID)

    if nodes.size == total_nodes:  # nó está pronto, logo avisa aos outros
        client.publish("ack-join", nodeID)

    sleep(0.5)  # espera um tempo

# Obtem antecessor e sucessor
nodes.sort()
index = np.where(nodes == nodeID)[0][0]
ant, suc = (index - 1, index + 1)  # antecessor e sucessor
if suc == total_nodes:
    suc = 0

print("Pronto! Intervalo de responsabilidade: (", int(nodes[ant]), ",", int(nodes[index]), "]")
# Se inscreve no topico put e get
client.subscribe("put")
client.subscribe("get")

while True: 
    continue
