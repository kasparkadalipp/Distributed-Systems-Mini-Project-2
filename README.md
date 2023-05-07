# Building a distributed system with underlying chain replication

This project was implemented as part of the distributed systems course by following authors:
- Kaspar Kadalipp
- Daniel Würsch
- Joshua Katigbak

## Requirements

Required applications:
* [etcd](https://etcd.io/)

Required python libraries:
* [etcd3](https://pypi.org/project/etcd3/)
* protobuf
* grpcio

## Running

### Generating protocol classes

GRPC protocol classes can be generated using following command:

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. bookshop.proto
```

### Nodes

Nodes can be started with following command:

```
python node.py port [etcd_host:etcd_port]
```

`etcd` host and port can be omitted in which case `localhost:2379` will be used.

Note, there is no limitation for the number of nodes which are connecting.
