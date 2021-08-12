import paho.mqtt.client as mqtt
from random import randrange
import sys


# MUITA COISA PRA TRATAR AINDA


def on_message(client, userdata, msg):
    topic = msg.topic
    m = message.payload.decode("utf-8")
    # se topic = put ou get fazer:
    # client.publish("putOk", True)  --- sei la o que retornar??
    # client.publish("getOk", hashTable[int(m)]) --se tiver dentro da faixa de posicoes que ele ta responsavel


rangeAddr = 2**32
hashTable = {}
nodeID = randrange(0,rangeAddr)

mqttBroker = "127.0.0.1" 

# Conecta ao broker mqtt
client = mqtt.Client("Node_%s" % sys.argv) # passar como parametro o nome/numero do nó
client.connect(mqttBroker)

# Publica seu nodeID no topic "join"
client.publish("join", nodeID)

# Se inscreve para receber os outros 7 nodeID's
client.subscribe("join")
client.on_message = on_message

# Se inscreve no topico put e get
client.subscribe("put")
client.subscribe("get")

client.loop_forever()

