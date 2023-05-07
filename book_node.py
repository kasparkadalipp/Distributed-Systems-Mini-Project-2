import sys
from concurrent import futures
import random
import grpc
import bookshop_pb2
import bookshop_pb2_grpc
from etcd_service import Etcd, get_host_ip
from datastore import DataStore


class Node(bookshop_pb2_grpc.NodeServiceServicer):
    def __init__(self, node_port, etcd_host="localhost", etcd_port=2379):
        self.datastores = []
        self.chain = []
        self.removed_heads = []
        self.timeout = 60
        self.etcd = Etcd(etcd_host, etcd_port)
        self.node_id = self.etcd.generate_node_id()
        self.node_port = node_port
        self.address = f"{get_host_ip()}:{node_port}"
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        bookshop_pb2_grpc.add_NodeServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{self.node_port}")
        self.server.start()
        self.etcd.register_node(self.node_id, self.address)
        print(f"Started Node-{self.node_id}")

    def BroadcastChain(self, request, context):
        self.chain = request.chain
        self.removed_heads = []
        for datastore in self.datastores:
            # evict existing data after new chain has been created
            datastore.books = {}
        raise bookshop_pb2.BroadcastChainResponse()

    def BroadcastRemoveHead(self, request, context):
        self.removed_heads.insert(0, self.chain[0])
        self.chain = self.chain[1:]
        raise bookshop_pb2.BroadcastRemoveHeadResponse()

    def BroadcastRestoreHead(self, request, context):
        self.chain.insert(0, self.removed_heads[0])
        self.removed_heads = self.removed_heads[1:]
        raise bookshop_pb2.BroadcastRestoreHeadResponse()

    def create_chain(self, command):
        if self.chain and command[1] != "force":
            print("Chain already created")
            return

        chain = list(self.etcd.datastores())
        random.shuffle(chain)
        chain_str = " -> ".join([f"Node{ds.node_id}-PS{ds.process_id}" for ds in chain])
        print(f"Broadcasting new chain:\n{chain_str}")
        channels = [grpc.insecure_channel(address) for (node_id, address) in self.etcd.nodes()]
        with futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
            _futures = []
            for channel in channels:
                stub = bookshop_pb2_grpc.NodeServiceStub(channel)
                request = bookshop_pb2.BroadcastChainRequest(chain=chain)
                _futures.append(executor.submit(stub.BroadcastChain, request))
            futures.wait(_futures, timeout=self.timeout)
        for channel in channels:
            channel.close()

    def list_chain(self, command: object) -> object:
        if not self.chain:
            print("Chain has not been created")
            return
        chain = [f"Node{ds.node_id}-PS{ds.process_id}" for ds in self.chain]
        chain[0] = f"{chain[0]} (Head)"
        chain[-1] = f"{chain[-1]} (Tail)"
        print(" -> ".join(chain))

    def list_books(self, command):
        if not self.chain:
            print("Chain has not been created")
            return
        datastore = random.choice(self.datastores)
        print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
        request = bookshop_pb2.ListRequest(allow_dirty=False)
        with grpc.insecure_channel(datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            response = stub.List(request, timeout=self.timeout)
        if not response.books:
            print("No books in stock")
            return "No books in stock"
        else:
            r = ""
            for i, book in enumerate(response.books):
                print("{:d}) {} = {:.2f}".format(i + 1, book.book, book.price))
                r += "{:d}) {} = {:.2f}".format(i + 1, book.book, book.price) + "\n"
            return r

    def data_status(self, command):
        if not self.chain:
            print("Chain has not been created")
            return "Chain has not been created"
        if self.datastores:
            datastore = random.choice(self.datastores)
            print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
            request = bookshop_pb2.ListRequest(allow_dirty=False)
            with grpc.insecure_channel(datastore.address) as channel:
                stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                response = stub.List(request, timeout=self.timeout)
                result = ""
            if not response.books:
                print("No books in stores")
                return "No books in stores"
            else:
                for i, book in enumerate(response.books):
                    print("{:d}) {} = {:.2f}".format(i + 1, book.book, book.price))
                    result += "{:d}) {} = {:.2f}".format(i + 1, book.book, book.price) + "\n"
                return result
        return "No books in stores"

    def remove_head(self, command):
        if not self.chain:
            print("Chain has not been created")
            return
        channels = [grpc.insecure_channel(address) for (node_id, address) in self.etcd.nodes()]
        with futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
            _futures = []
            for channel in channels:
                stub = bookshop_pb2_grpc.NodeServiceStub(channel)
                request = bookshop_pb2.BroadcastRemoveHeadRequest()
                _futures.append(executor.submit(stub.BroadcastRemoveHead, request))
            futures.wait(_futures, timeout=self.timeout)
        for channel in channels:
            channel.close()
        print("Broadcasted removal of head")

    def restore_head(self, command):
        if not self.chain:
            print("Chain has not been created")
            return "Chain has not been created"
        if not self.removed_heads:
            print("No head to be restored")
            return "No head to be restored"
        head = self.removed_heads[0]
        print(f"Attempting to restore Node{head.node_id}-PS{head.process_id}")
        request = bookshop_pb2.RestoreHeadRequest()
        with grpc.insecure_channel(head.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            response = stub.RestoreHead(request, timeout=self.timeout)
        if response.successful:
            print(f"Successfully instituted Node{head.node_id}-PS{head.process_id} as new master")
            return f"Successfully instituted Node{head.node_id}-PS{head.process_id} as new master"
        else:
            print(f"Failure restoring Node{head.node_id}-PS{head.process_id}")
            return f"Failure restoring Node{head.node_id}-PS{head.process_id}"


    def local_store_ps(self, command):

        if self.datastores:
            print("Data stores already created")
        else:
            num_processes = int(command[1])
            self.datastores = [DataStore(self, process_id) for process_id in range(1, num_processes + 1)]

    def read_operation(self, command):

        if not self.chain:
            print("Chain has not been created")
            return
        if not self.datastores:
            print("Data stores have not been created")
            return "Data stores have not been created"
        datastore = random.choice(self.datastores)
        print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
        request = bookshop_pb2.ReadRequest(books=[command[1]], allow_dirty=False)
        with grpc.insecure_channel(datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            response = stub.Read(request, timeout=self.timeout)
        if not response.books:
            print("Not yet in stock")
            return "Not yet in stock"
        else:
            for book in response.books:
                print("{} = {:.2f}".format(book.book, book.price))
            return response.books

    def write_operation(self, command):

        if not self.chain:
            print("Chain has not been created")
            return
        price, book = [s[::-1].strip() for s in command[1][1:-1][::-1].split(",", 1)]
        price = float(price)
        head = self.chain[0]
        print(f"Write to Node{head.node_id}-PS{head.process_id}")
        request = bookshop_pb2.WriteRequest(book=book, price=price)
        with grpc.insecure_channel(head.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            response = stub.Write(request, timeout=self.timeout)
        print("{} = {:.2f}".format(response.book.book, response.book.price))

    def set_timeout(self, command):
        self.timeout = int(command[1]) * 60
        print(f"Timeout = {self.timeout}min")

    def accept_user_input(self):
        sys.stdout.write(f"Node-{self.node_id}> ")
        command = input().strip().split(" ", 1)
        commands = {
            'create-chain': self.create_chain,
            'list': self.list_books,
            'data-status': self.data_status,
            'remove-head': self.remove_head,
            'restore-head': self.restore_head,
            'local-store-ps': self.local_store_ps,
            'read': self.read_operation,
            'write': self.write_operation,
            'set-timeout': self.set_timeout
        }
        if command[0].lower() in commands:
            c = commands[command[0].lower()]
            c(command)
        else:
            print("Invalid command")


def main():
    if len(sys.argv) == 2:
        node_port = int(sys.argv[1])
        etcd_host, etcd_port = "localhost", 2379
    elif len(sys.argv) == 3:
        node_port = int(sys.argv[1])
        etcd_host, etcd_port = sys.argv[2].split(":", 1)
        etcd_port = int(etcd_port)
    else:
        sys.exit(f"Usage: {sys.argv[0]} node-port [etcd-host:etcd-port]")

    node = Node(etcd_host, etcd_port, node_port)
    while True:
        try:
            node.accept_user_input()
        except KeyboardInterrupt:
            print("Exiting...")
            break


# main()
