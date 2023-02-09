import grpc
import machbase_proto_pb2 as machbase_proto_pb2
import machbase_proto_pb2_grpc as machbase_proto_pb2_grpc
from google.protobuf.any_pb2 import Any
import google.protobuf.wrappers_pb2 as pb_wp

create_tb = "create tag table grpc1 (name varchar(20) primary key, time datetime basetime, value double summarized)"
drop_tb = "drop table grpc1"
channel = grpc.insecure_channel('127.0.0.1:5655')
mach_stub = machbase_proto_pb2_grpc.MachbaseStub(channel)
response = mach_stub.Exec(machbase_proto_pb2.ExecRequest(sql=create_tb))

DataStream = [["tag0", 1675831590, 1.234]\
             ,["tag0", 1675831591, 5.678]\
             ,["tag0", 1675831592, 3.421]\
             ,["tag0", 1675831593, 4.432]]

def To_Anylist(row: list):
    AnyTag, AnyTime, AnyValue = Any(), Any(), Any()
    AnyTag.Pack(pb_wp.StringValue(value = str(row[0])))
    AnyTime.Pack(pb_wp.Int64Value(value = int(row[1])))
    AnyValue.Pack(pb_wp.FloatValue(value = float(row[2])))
    return [AnyTag, AnyTime, AnyValue]

def To_AppendStream(Anylist: list[list[Any]], handle: str):
    for row in Anylist:
        yield machbase_proto_pb2.AppendData(handle = handle, params = row)

Appr_response = mach_stub.Appender(machbase_proto_pb2.AppenderRequest(tableName = "grpc1"))
print("Appender create Success: %s"%Appr_response.success)


Anylist = list()
for row in DataStream:
    Anylist.append(To_Anylist(row))
    
response = mach_stub.Append(To_AppendStream(Anylist, Appr_response.handle))
print("Append data Success: %s"%response.success)
