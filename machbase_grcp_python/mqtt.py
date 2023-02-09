import paho.mqtt.client as mqtt
import time
import json
import random

broker = '127.0.0.1'
broker_port = 5653
username = 'user'
passwd = 'pass'
keepalive = 60
cleansession = True
clientid = f"machbase-mqtt-cli-{random.randint(0,1000)}"
connected_flag = False
subscription_flag = False
publish_flag = False
message_flag = False

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            global connected_flag
            connected_flag = True
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n" %rc)
            return -1
    client = mqtt.Client(clientid, clean_session=cleansession)
    client.username_pw_set(username, passwd)
    client.on_connect = on_connect
    client.connect(broker, broker_port, keepalive=keepalive)
    client.loop_start()
    print("Connection process starts.\n")
    while not connected_flag:
        print("Waiting for connection.\n")
        time.sleep(1)

    client.loop_stop()
    return client

def publish(client, msg, pub_topic):
    if msg == 0:
        print("no message!\n")

    def on_publish(client, userdata, mid):
        global publish_flag
        publish_flag = True
        print("Published: messageid " + str(mid) + ". Send " + str(len(msg)) + " to topic " + str(pub_topic) +"\n")

    client.on_publish = on_publish
    result = client.publish(pub_topic, msg, 1, False)
    client.loop_start()

    count = 0
    while not publish_flag:
        count += 1
        if (count == 100):
            print("Waiting for publishing.")
            count = 0

    client.loop_stop()
        
def subscribe(client, buff, sub_topic):
    msg_count = 0
    def on_subscribe(client, userdata, mid, granted_qos):
        global subscription_flag
        subscription_flag = True
        print("Subscribed: " + str(mid) + " " + str(granted_qos))
    def on_message(client, userdata, msg):
        global message_flag
        message_flag = True
        print("Received data from"+ msg.topic)
        buff.append(str(msg.payload))
        client.loop_stop()
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    result = client.subscribe(sub_topic,1)
    print("Reciving message process starts.\n")
    client.loop_start()
    while not subscription_flag:
        print("Waiting for subscription.\n")
        time.sleep(1)
    client.loop_stop()
    client.loop_start()

