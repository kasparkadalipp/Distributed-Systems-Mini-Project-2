import sys
import concurrent
import random
import socket

import etcd3
import grpc

import bookshop_pb2
import bookshop_pb2_grpc

# Hackish way to get the address to bind to.
# Error prone, as computers tend to have multiple interface,
# but good enough for this project...
def get_host_ip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

class Etcd():
    def __init__(self, etcd_host, etcd_port):
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)

    def generate_node_id(self):
        # initialize counter variable if it does not already exist
        self.etcd.transaction(
            compare=[etcd3.transactions.Version('/node_counter') == 0],
            success=[etcd3.transactions.Put('/node_counter', '1')],
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
            yield node_id, address

    def datastores(self):
        """Returns all nodes registered in the cluster as a tuple containing the node id and the address"""
        for address, meta in self.etcd.get_prefix("/datastores/"):
            node_id = int(meta.key.decode().split("/")[-2])
            process_id = int(meta.key.decode().split("/")[-1])
            yield bookshop_pb2.Datastore(node_id = node_id, process_id = process_id, address = address)

class Node(bookshop_pb2_grpc.NodeServiceServicer):
    def __init__(self, etcd_host, etcd_port, node_port):
        self.datastores = []
        self.chain = []
        self.removed_heads = []
        self.timeout = 60
        self.etcd = Etcd(etcd_host, etcd_port)
        self.node_id = self.etcd.generate_node_id()
        self.node_port = node_port
        self.address = f"{get_host_ip()}:{node_port}"
        self.server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
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

    def accept_user_input(self):
        sys.stdout.write(f"Node-{self.node_id}> ")
        command = input().strip().split(" ",1)
        match command[0].lower():
            case 'create-chain':
                if self.chain:
                    print("Chain already created, so you want to re-create it? [N/y]")
                    if input().strip().lower() != "y":
                        return
                chain = list(self.etcd.datastores())
                random.shuffle(chain)
                chain_str = " -> ".join([f"Node{ds.node_id}-PS{ds.process_id}" for ds in chain])
                print(f"Broadcasting new chain:\n{chain_str}")
                channels = [grpc.insecure_channel(address) for (node_id, address) in self.etcd.nodes()]
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
                    futures = []
                    for channel in channels:
                        stub = bookshop_pb2_grpc.NodeServiceStub(channel)
                        request = bookshop_pb2.BroadcastChainRequest(chain = chain)
                        futures.append(executor.submit(stub.BroadcastChain, request))
                    concurrent.futures.wait(futures, timeout=self.timeout)
                for channel in channels:
                    channel.close()
            case 'list-chain':
                if not self.chain:
                    print("Chain has not been created")
                    return
                chain = [f"Node{ds.node_id}-PS{ds.process_id}" for ds in self.chain]
                chain[0] = chain[0] + " (Head)"
                chain[-1] = chain[-1] + " (Tail)"
                print(" -> ".join(chain))
            case 'list-books':
                if not self.chain:
                    print("Chain has not been created")
                    return
                datastore = random.choice(self.datastores)
                print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
                request = bookshop_pb2.ListRequest(allow_dirty = False)
                with grpc.insecure_channel(datastore.address) as channel:
                    stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                    response = stub.List(request, timeout = self.timeout)
                if not response.books:
                    print("No books in stock")
                else:
                    for i, book in enumerate(response.books):
                        print("{:d}) {} = {:.2f}".format(i, book.book, book.price))
            case 'data-status':
                if not self.chain:
                    print("Chain has not been created")
                    return
                datastore = random.choice(self.datastores)
                print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
                request = bookshop_pb2.ListRequest(allow_dirty = True)
                with grpc.insecure_channel(datastore.address) as channel:
                    stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                    response = stub.List(request, timeout = self.timeout)
                if not response.books:
                    print("No books in stock")
                else:
                    for i, book in enumerate(response.books):
                        print("{:d}) {} = {:.2f} ({})".format(i+1, book.book, book.price, "dirty" if book.dirty else "clean"))
            case 'remove-head':
                if not self.chain:
                    print("Chain has not been created")
                    return
                channels = [grpc.insecure_channel(address) for (node_id, address) in self.etcd.nodes()]
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
                    futures = []
                    for channel in channels:
                        stub = bookshop_pb2_grpc.NodeServiceStub(channel)
                        request = bookshop_pb2.BroadcastRemoveHeadRequest()
                        futures.append(executor.submit(stub.BroadcastRemoveHead, request))
                    concurrent.futures.wait(futures, timeout=self.timeout)
                for channel in channels:
                    channel.close()
                print("Broadcasted removal of head")
            case 'restore-head':
                if not self.chain:
                    print("Chain has not been created")
                    return
                if not self.removed_heads:
                    print("No head to be restored")
                    return
                head = self.removed_heads[0]
                print(f"Attempting to restore Node{head.node_id}-PS{head.process_id}")
                request = bookshop_pb2.RestoreHeadRequest()
                with grpc.insecure_channel(head.address) as channel:
                    stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                    response = stub.RestoreHead(request, timeout = self.timeout)
                if response.successful:
                    print(f"Successfully instituted Node{head.node_id}-PS{head.process_id} as new master")
                else:
                    print(f"Failure restoring Node{head.node_id}-PS{head.process_id}")

            case 'local-store-ps':
                if self.datastores:
                    print("Data stores already created")
                else:
                    num_processes = int(command[1])
                    self.datastores = [DataStore(self, process_id) for process_id in range(1, num_processes+1)]
            case 'read-operation':
                if not self.chain:
                    print("Chain has not been created")
                    return
                datastore = random.choice(self.datastores)
                print(f"Read from Node{self.node_id}-PS{datastore.process_id}")
                request = bookshop_pb2.ReadRequest(books = [command[1]], allow_dirty = False)
                with grpc.insecure_channel(datastore.address) as channel:
                    stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                    response = stub.Read(request, timeout = self.timeout)
                if not response.books:
                    print("Not yet in stock")
                else:
                    for book in response.books:
                        print("{} = {:.2f}".format(book.book, book.price))
            case 'write-operation':
                if not self.chain:
                    print("Chain has not been created")
                    return
                price, book = [s[::-1].strip() for s in command[1][1:-1][::-1].split(",",1)]
                price = float(price)
                head = self.chain[0]
                print(f"Write to Node{head.node_id}-PS{head.process_id}")
                request = bookshop_pb2.WriteRequest(book = book, price = price)
                with grpc.insecure_channel(head.address) as channel:
                    stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                    response = stub.Write(request, timeout = self.timeout)
                print("{} = {:.2f}".format(response.book.book, response.book.price))
            case 'time-out':
                self.timeout = int(command[1])*60
                print(f"Timeout = {self.timeout}min")
            case 'ml-list-recommend':
                pass  # TODO
            case x:
                if x:
                    print(f"Unknown command '{x}'")

class DataStore(bookshop_pb2_grpc.DatastoreServiceServicer):
    def __init__(self, node, process_id):
        self.books = {}
        self.node = node
        self.process_id = process_id
        self.address = f"{get_host_ip()}:{node.node_port+process_id}"
        self.server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
        bookshop_pb2_grpc.add_DatastoreServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{node.node_port+process_id}")
        self.server.start()
        node.etcd.register_datastore(node.node_id, process_id, self.address)
        print(f"Started Node-{node.node_id}-PS{process_id}")

    def Write(self, request, context):
        entry = bookshop_pb2.Book(book = request.book, price = request.price, dirty = True)
        self.books[request.book] = entry # add dirty entry to current store
        for datastore in self.tail():
            with grpc.insecure_channel(datastore.address) as channel:
                stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                try:
                    response = stub.Write(request, timeout = self.node.timeout)
                    break
                except Exception as e:
                    print(f"Failure during write operation: {e}") # Continue to next in chain.
        entry.dirty = False # mark entry ad non-dirty, change has been propagated in chain
        return bookshop_pb2.WriteResponse(book = entry)

    def Read(self, request, context):
        books = [self.books[book] for book in request.books if book in self.books]
        dirty_books = [book for book in books if book.dirty]
        if dirty_books and not request.allow_dirty:
            # we have dirty reads, checking with tail to get fully committed changes for dirty reads
            tail = self.node.chain[-1]
            request = bookshop_pb2.ReadRequest(books = dirty_books, allow_dirty = False)
            with grpc.insecure_channel(tail.address) as channel:
                stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                response = stub.Read(request, timeout = self.node.timeout)
            books = [book for book in books if not book.dirty] + response.books
        return bookshop_pb2.ReadResponse(books = books)

    def List(self, request, context):
        request = bookshop_pb2.ReadRequest(books = self.books.keys(), allow_dirty = request.allow_dirty)
        response = self.Read(request, context)
        return bookshop_pb2.ListResponse(books = response.books)

    def RestoreHead(self, request, context):
        # TODO: Attempt to resync this datastore to become new master and set variable successful accordingly
        successful = False
        if successful:
            channels = [grpc.insecure_channel(address) for (node_id, address) in self.node.etcd.nodes()]
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(channels)) as executor:
                futures = []
                for channel in channels:
                    stub = bookshop_pb2_grpc.NodeServiceStub(channel)
                    request = bookshop_pb2.BroadcastRestoreHeadRequest()
                    futures.append(executor.submit(stub.BroadcastRestoreHead, request))
                concurrent.futures.wait(futures, timeout = self.node.timeout)
            for channel in channels:
                channel.close()
        return bookshop_pb2.RestoreHeadResponse(successful = successful)

    def tail(self):
        tail = False
        for datastore in self.node.chain:
            if tail:
                yield datastore
            elif datastore.address == self.address:
                tail = True

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

if __name__ == '__main__':
    main()
