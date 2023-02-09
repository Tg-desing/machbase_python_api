import time
import json
import mqtt


def CreateMqttJson(f, rows_count):
    rows_list = list()
    for i in range(rows_count):
        oneline = f.readline()
        data_list = oneline[:-1].split(',')
        data_list[1] = int(data_list[1])
        data_list[2] = float(data_list[2])
        rows_list.append(data_list)
        if (len(rows_list) > 10000):
            yield rows_list
            rows_list.clear()
    yield rows_list

client = mqtt.connect_mqtt()

print("MQTT input starts")
start = time.time()
with open("eqp_mod8.csv", "r") as f:
    start = time.time()
    for rows_list in CreateMqttJson(f, 54 * 5000):
        mqtt.publish(client, json.dumps(rows_list), "db/append/MQTT")
    stop = time.time()
print("MQTT success: elapsed time : %f"%(stop - start))
        
client.disconnect()