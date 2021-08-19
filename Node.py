import paho.mqtt.client as mqtt
from random import randrange
import sys
import numpy as np
from time import sleep

nodes = np.array([])  # para guardar os nodesId
ack_nodes = np.array([])  # para guardar os nós prontos


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
    # else
    # se topic = put ou get fazer:
    # client.publish("putOk", True)  --- sei la o que retornar??
    # client.publish("getOk", hashTable[int(m)]) --se tiver dentro da faixa de posicoes que ele ta responsavel


rangeAddr = 2 ** 32
hashTable = {}
nodeID = randrange(0, rangeAddr)

mqttBroker = "127.0.0.1"

# Conecta ao broker mqtt
client = mqtt.Client("Node_%s" % sys.argv[1])  # passar como parametro o nome/numero do nó
while client.connect(mqttBroker) != 0:
    sleep(0.1)
print("Node_%s conectado ao broker." % sys.argv[1], "ID: " + str(nodeID))

# Se inscreve para receber os outros 7 nodeID's
client.subscribe("join")
client.subscribe("ack-join")
client.on_message = on_message

print("Preparando para receber mensagens...")
# Publica seu nodeID no topic "join"
while nodes.size < 8 or ack_nodes.size < 8:  # enquanto nao esta pronto ou os outros nós nao estao
    client.publish("join", nodeID)

    if nodes.size == 8:  # nó está pronto, logo avisa aos outros
        client.publish("ack-join", nodeID)

    sleep(0.5)  # espera um tempo

# Obtem antecessor e sucessor
nodes.sort()
index = np.where(nodes == nodeID)
ant, suc = (index - 1, index + 1)  # antecessor e sucessor
if suc == 8: suc = 0

print("Pronto!")
# Se inscreve no topico put e get
client.subscribe("put")
client.subscribe("get")

client.loop_forever()