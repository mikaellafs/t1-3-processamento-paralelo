from random import randrange
from django.utils.crypto import get_random_string

import paho.mqtt.client as mqtt


def on_message(client, userdata, msg):
    m = msg.payload.decode("utf-8")
    if msg.topic == "ack-put":
        print("Added value on key " + m)
        return
    if msg.topic == "res-get":
        print("Received value: " + m)
        return


rangeAddr = 2 ** 32
mqttBroker = "127.0.0.1"

client = mqtt.Client("Cliente")
client.connect(mqttBroker)

client.subscribe("ack-put")  # acknowledgement-put()
client.subscribe("res-get")  # response-get()

client.on_message = on_message

keys = {}
keysQtde = round(0.00005 * rangeAddr)

for i in range(0, keysQtde):

    key = randrange(0, rangeAddr)
    value = get_random_string(10)

    msg = str(key) + " " + value
    client.publish("put", msg)
    keys[i] = key

    print("(" + "{:.3f}".format((i / keysQtde) * 100), "%) ", end='')
    print("Just published \'" + msg + "\' to topic \'put\'")
