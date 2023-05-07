import socket
import etcd3
import bookshop_pb2


def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


class Etcd:
    def __init__(self, etcd_host, etcd_port):
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)

    def generate_node_id(self):
        # initialize counter variable if it does not already exist
        self.etcd.transaction(
            compare=[self.etcd.transactions.version('/node_counter') == 0],
            success=[self.etcd.transactions.put('/node_counter', '1')],
            failure=[]
        )
        # atomically get and increment variable
        increment_successful = False
        while not increment_successful:
            counter = int(self.etcd.get('/node_counter')[0])
            increment_successful = self.etcd.replace(
                '/node_counter', str(counter), str(counter + 1))
        return counter

    def register_node(self, node_id, address):
        self.etcd.put(f"/nodes/{node_id}", address)

    def register_datastore(self, node_id, process_id, address):
        self.etcd.put(f"/datastores/{node_id}/{process_id}", address)

    def nodes(self):
        """Returns all nodes registered in the cluster as a tuple containing the node id and the address"""
        for address, meta in self.etcd.get_prefix("/nodes/"):
            node_id = int(meta.key.decode().split("/")[-1])
            yield node_id, str(address.decode())

    def datastores(self):
        """Returns all nodes registered in the cluster as a tuple containing the node id and the address"""
        for address, meta in self.etcd.get_prefix("/datastores/"):
            node_id = int(meta.key.decode().split("/")[-2])
            process_id = int(meta.key.decode().split("/")[-1])
            yield bookshop_pb2.Datastore(node_id=node_id, process_id=process_id, address=address)

    def clear(self):
        self.etcd.delete_prefix("/")
