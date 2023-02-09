import grpc
import machbase_proto_pb2 as machbase_proto_pb2
import machbase_proto_pb2_grpc as machbase_proto_pb2_grpc
from google.protobuf.any_pb2 import Any
import google.protobuf.wrappers_pb2 as pb_wp
import google.protobuf.timestamp_pb2 as pb_ts

channel = grpc.insecure_channel('127.0.0.1:5655')
mach_stub = machbase_proto_pb2_grpc.MachbaseStub(channel)

create_tb = "create tag table grpc1 (name varchar(20) primary key, time datetime basetime, value double summarized)"
drop_tb = "drop table grpc1"
insert_tt = "insert into grpc1 (name, time, value) values (?, ?, ?)"
select_qy = "select * from grpc1"
count_qy = "select count(*) from grpc1"

# create a table
response = mach_stub.Exec(machbase_proto_pb2.ExecRequest(sql=create_tb))
print("Creating table Success: %s"%response.success)

Tag = pb_wp.StringValue()
Tag.value = str("tag1")
Time = pb_wp.Int64Value()
Time.value= int(1)
Value = pb_wp.FloatValue()
Value.value = float(1.234)


AnyTag = Any()
AnyTag.Pack(Tag)

AnyTime = Any()
AnyTime.Pack(Time)

AnyValue= Any()
AnyValue.Pack(Value)

# insert a row
for i in range(5):
    ExecResponse = mach_stub.Exec(machbase_proto_pb2.ExecRequest(sql = insert_tt, params= [AnyTag, AnyTime, AnyValue]))
    print("Inserting data: %s"%ExecResponse.reason)

# Query (select * from grpc)
QueryResponse = mach_stub.Query(machbase_proto_pb2.QueryRequest(sql = select_qy))
RowsFetchResponse = mach_stub.RowsFetch(QueryResponse.rowsHandle)
sqlresponse = list()

while (not RowsFetchResponse.hasNoRows):

    for rows in RowsFetchResponse.values:
        if rows.type_url == "type.googleapis.com/google.protobuf.Timestamp":
            Unpack_obj = pb_ts.Timestamp()
            rows.Unpack(Unpack_obj)
            sqlresponse.append((int(Unpack_obj.seconds) + int(Unpack_obj.nanos * 1000000000)))
        elif rows.type_url == "type.googleapis.com/google.protobuf.Int64Value":
            Unpack_obj = pb_wp.Int64Value()
            rows.Unpack(Unpack_obj)
            sqlresponse.append(int(Unpack_obj.value))
        elif rows.type_url == "type.googleapis.com/google.protobuf.DoubleValue":
            Unpack_obj = pb_wp.DoubleValue()
            rows.Unpack(Unpack_obj)
            sqlresponse.append(float(Unpack_obj.value))
        elif rows.type_url == "type.googleapis.com/google.protobuf.StringValue":
            Unpack_obj = pb_wp.StringValue()
            rows.Unpack(Unpack_obj)
            sqlresponse.append(str(Unpack_obj.value))

    print(sqlresponse)
    sqlresponse.clear()
    RowsFetchResponse = mach_stub.RowsFetch(QueryResponse.rowsHandle)
 
RowsCloseSuccess = mach_stub.RowsClose(QueryResponse.rowsHandle)
print("Rows Handle Close: " + RowsCloseSuccess.reason)

# QueryRow 
QueryRowResponse = mach_stub.QueryRow(machbase_proto_pb2.QueryRowRequest(sql = count_qy))
for rows in QueryRowResponse.values:
    if rows.type_url == "type.googleapis.com/google.protobuf.Int64Value":
        ValueResponse = pb_wp.Int64Value()
        rows.Unpack(ValueResponse) 
        print("QueryRow results: {0:d}".format(ValueResponse.value))

# Drop table
response = mach_stub.Exec(machbase_proto_pb2.QueryRowRequest(sql = drop_tb))
print("Dropping table Sucess: %s"%response.reason)
