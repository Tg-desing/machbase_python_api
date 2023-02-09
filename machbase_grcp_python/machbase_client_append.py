import grpc
import machbase_proto_pb2 
import machbase_proto_pb2_grpc
import time
from google.protobuf.any_pb2 import Any
import google.protobuf.wrappers_pb2 as pb_wp


def To_Any_list(row: list[str]):
    Tag = pb_wp.StringValue()
    Tag.value = str(row[0])
    Time = pb_wp.Int64Value()
    Time.value = int(row[1])
    Value = pb_wp.FloatValue()
    Value.value = float(row[2])
    AnyTag = Any()
    AnyTag.Pack(Tag)
    AnyTime = Any()
    AnyTime.Pack(Time)
    AnyValue= Any()
    AnyValue.Pack(Value)
    return [AnyTag, AnyTime, AnyValue]

def To_Append_stream(rows: list[list[Any]], handle: str):
    Append_stream = list()
    for row in rows:
        Append_stream.append(machbase_proto_pb2.AppendData(handle = handle, params = row))
    
    for i in Append_stream:
        yield i


channel = grpc.insecure_channel('127.0.0.1:5655')
mach_stub = machbase_proto_pb2_grpc.MachbaseStub(channel)

create_tb = "create tag table grpc1 (name varchar(20) primary key, time datetime basetime, value double summarized)"
response = mach_stub.Exec(machbase_proto_pb2.ExecRequest(sql=create_tb))
print("Creating table Success: %s"%response.success)

response = mach_stub.Appender(machbase_proto_pb2.AppenderRequest(tableName = "grpc1"))
print("Appender create Success: %s"%response.success)

handle = response.handle

with open("eqp_mod8.csv", "r") as f:
    Any_list = list()
    for k in range(54 * 5000):
        oneline = f.readline()
        row_list = oneline[:-1].split(",")
        Any_list.append(To_Any_list(row_list))
        

start_time = time.time()
response = mach_stub.Append(To_Append_stream(Any_list, handle))
end_time = time.time()
print("Append data Success: %s"%response.success)
print("Elapsed time is %f"%(end_time - start_time))