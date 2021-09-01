from random import randrange
from time import sleep

from django.utils.crypto import get_random_string

import paho.mqtt.client as mqtt
import numpy as np

keys = np.array([])
values = np.array([])
ack_received = 0


def on_message(client, userdata, msg):

    m = msg.payload.decode("utf-8")

    if msg.topic == "ack-put":
        global ack_received
        ack_received += 1
        print("Added value to nodeID " + m)
        return

    if msg.topic == "res-get":
        global values
        values = np.append(values, m)
        print("Received value: " + m)
        return


rangeAddr = 2 ** 32
mqttBroker = "127.0.0.1"

client = mqtt.Client("Cliente")
client.connect(mqttBroker)

client.subscribe("ack-put")  # acknowledgement-put()
client.subscribe("res-get")  # response-get()

client.on_message = on_message

keysQtde = 100
client.loop_start()

for i in range(1, keysQtde + 1, 1):

    key = str(randrange(0, rangeAddr))
    value = get_random_string(10)

    msg = key + " " + value
    client.publish("put", msg)
    keys = np.append(keys, key)

    print("(" + "{:.3f}".format((i / keysQtde) * 100), "%) ", end='')
    print("Just published \'" + msg + "\' to topic \'put\'")

while ack_received < keysQtde:
    sleep(1)

for key in keys:
    client.publish("get", key)

while values.size < keysQtde:
    sleep(1)

client.loop_stop()
