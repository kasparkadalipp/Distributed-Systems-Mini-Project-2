# Building a distributed system with underlying chain replication

This project was implemented as part of the distributed systems course by following authors:
- Kaspar Kadalipp
- Daniel Würsch
- Joshua Katigbak

## Requirements

### Dependencies

Required applications:
* [etcd](https://etcd.io/)

Required python libraries:
* [etcd3](https://pypi.org/project/etcd3/)
* protobuf
* grpcio


### Generating protocol classes

GRPC protocol classes can be generated using following command:

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. bookshop.proto
```

## Implementation Decisions

### etcd

In order to avoid hard-coding addresses (in code or as program argument), we opted to use etcd as simple "service discovery". Nodes and datastores will put their address (host and port) to etcd. The `create-chain` operation will then be able to gather all available nodes and datastores from etcd.

### read-operation, list-book

The exercise specifies that read operations should consult with the head to return the most recent entry. However, this will return dirty data which hasn't been fully committed and will prevent scaling read operations, as all reads inevitably will be performed on the head.

Instead, we chose to not return any dirty data which has not been confirmed by the tail, i.e. only fully committed data confirmed by the tail will be returned. This allows better scaling for read operations, because any node can handle read operations and only needs to consult with the tail node for entries which are being committed at the very same time, i.e. have been marked as dirty.

### recover-node
Using vector clocks of Lamport clocks to calculate the numerical deviation of operations as proposed in the task description is sub-optimal, because read-only operations will still modify the clocks and potentially prevent a recover operations.

Because our storage does not allow deletion of entries, it is possible to simply version the entries themselves, then the node can calculate the deviation based on the versions of the entries, taking into account only write operations which have been performed.

## Running

### etcd

etcd needs to be available for nodes and should be started. In case nodes are remote, etcd needs to be started to allow remote connections:

```
etcd --listen-client-urls 'http://0.0.0.0:2379' --advertise-client-urls 'http://0.0.0.0:2379'
```

### Nodes

Nodes can be started with following command:

```
python node.py port [etcd_host:etcd_port]
```

`etcd_host:etcd_port` can be omitted in which case `localhost:2379` will be used.

Note, there is no limitation for the number of nodes which are connecting.


![Architecture Blueprint.png](https://imgur.com/a/MelxP0n)
