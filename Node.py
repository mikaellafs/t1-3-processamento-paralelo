import paho.mqtt.client as mqtt
from random import randrange
import sys
import numpy as np
from time import sleep

nodes = np.array([])  # para guardar os nodesId


def on_message(client, userdata, msg):
    topic = msg.topic
    m = msg.payload.decode("utf-8")

    if topic == "join":
        global nodes
        m = int(m)  # é um node id, converte pra int
        nodes = np.unique(np.append(nodes, m))  # insere no array de nós, o unique retira repetidos
    # else
    # se topic = put ou get fazer:
    # client.publish("putOk", True)  --- sei la o que retornar??
    # client.publish("getOk", hashTable[int(m)]) --se tiver dentro da faixa de posicoes que ele ta responsavel


rangeAddr = 2 ** 32
hashTable = {}
nodeID = randrange(0, rangeAddr)
print(sys.argv[1] + ": " + str(nodeID))

mqttBroker = "127.0.0.1"

# Conecta ao broker mqtt
client = mqtt.Client("Node_%s" % sys.argv[1])  # passar como parametro o nome/numero do nó
client.connect(mqttBroker)

# Se inscreve para receber os outros 7 nodeID's
client.subscribe("join")
client.on_message = on_message

# Publica seu nodeID no topic "join"
while nodes.size < 8:  # total de nós
    client.publish("join", nodeID)
    sleep(0.5)  # espera um tempo

# Obtem antecessor e sucessor
index = np.where(nodes == nodeID)
ant, suc = (index - 1, index + 1)  # antecessor e sucessor

# Se inscreve no topico put e get
client.subscribe("put")
client.subscribe("get")

client.loop_forever()
