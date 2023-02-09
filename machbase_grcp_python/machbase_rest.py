import json
import requests
import time


def csv_to_list(file, rowcount :int):
    for i in range(rowcount):
        oneline = file.readline()
        data_list = oneline[:-1].split(',')
        data_list[1] = int(data_list[1])
        data_list[2] = float(data_list[2])
        yield data_list
            
def csv_to_json(csv: str, tb_name: str, rowcount: int):
    json_dict = dict()
    json_dict["table"] = tb_name
    data_dict = dict()
    data_dict["columns"] = ["name", "time", "value"]
    data_dict["rows"] = list()
    with open(csv, 'r') as f:
        for data in csv_to_list(f, rowcount):
            data_dict["rows"].append(data)
        json_dict["data"] = data_dict
    return json.dumps(json_dict)

def http_post(url, headers, data_json):
    print("Http request starts")
    start = time.time()
    response = requests.post(url, data= data_json, headers=headers)
    if response.status_code != 200:
        print("http Connection failed. Check exact http url") 
        response.raise_for_status()
    stop = time.time()
    print("Elapsed time : %f"%(stop - start))

TABLE = "RESTAPI"
CSVFILE_NAME = "eqp_mod8.csv"
URL ="http://127.0.0.1:5654/db/write"
HEADERS = {'Content-Type': 'application/json; charset=utf-8'}

data_json = csv_to_json(CSVFILE_NAME, TABLE, 54 * 5000)
http_post(URL, HEADERS, data_json)
