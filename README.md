# Building a distributed system with underlying chain replication

This project was implemented as part of the distributed systems course by following authors:
- Kaspar Kadalipp
- Daniel Würsch
- Joshua Katigbak

## Requirements

Required python libraries:
* protobuf
* grpcio

## Running

### Generating protocol classes

GRPC protocol classes can be generated using following command:

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. bookshop.proto
```
