import grpc
import machbase_proto_pb2.machbase_proto_pb2 as machbase_proto_pb2
import machbase_proto_pb2.machbase_proto_pb2_grpc as machbase_proto_pb2_grpc
from google.protobuf.any_pb2 import Any
import google.protobuf.wrappers_pb2 as pb_wp

channel = grpc.insecure_channel('127.0.0.1:5655')
mach_stub = machbase_proto_pb2_grpc.MachbaseStub(channel)

DataStream = [["tag0", 1675831590, 1.234]\
             ,["tag0", 1675831591, 5.678]\
             ,["tag0", 1675831592, 3.421]\
             ,["tag0", 1675831593, 4.432]]

def To_Any_list(row: list):
    Tag = pb_wp.StringValue()
    Tag.value = str(row[0])
    Time = pb_wp.Int64Value()
    Time.value = int(row[1])
    Value = pb_wp.FloatValue()
    Value.value = float(row[2])
    AnyTag = Any()
    AnyTime = Any()
    AnyValue = Any()
    AnyTag.Pack(Tag)
    AnyTime.Pack(Time)
    AnyValue.Pack(Value)
    return [AnyTag, AnyTime, AnyValue]

def To_AppendStream(Anylist: list[list[Any]], handle: str):
    for row in Anylist:
        yield machbase_proto_pb2.AppendData(handle = handle, params = row)

response = mach_stub.Appender(machbase_proto_pb2.AppenderRequest(tableName = "grpc"))
print("Appender create Success: %s"%response.success)
handle = response.handle

Anylist = list()
for row in DataStream:
    Anylist.append(To_Any_list(row))
    
response = mach_stub.Append(To_AppendStream(Anylist, handle))
print("Append data Success: %s"%response.success)
