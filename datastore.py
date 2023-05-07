from concurrent import futures
import grpc

import bookshop_pb2
import bookshop_pb2_grpc
from etcd_service import get_host_ip


class DataStore(bookshop_pb2_grpc.DatastoreServiceServicer):
    def __init__(self, node, process_id):
        self.books = {}
        self.node = node
        self.process_id = process_id
        self.address = f"{get_host_ip()}:{node.node_port + process_id}"
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        bookshop_pb2_grpc.add_DatastoreServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(f"[::]:{node.node_port + process_id}")
        self.server.start()
        node.etcd.register_datastore(node.node_id, process_id, self.address)
        print(f"Started Node-{node.node_id}-PS{process_id}")

    def Write(self, request, context):
        entry = bookshop_pb2.Book(book=request.book, price=request.price, dirty=True)
        self.books[request.book] = entry  # add dirty entry to current store
        for datastore in self.tail():
            with grpc.insecure_channel(datastore.address) as channel:
                stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                try:
                    response = stub.Write(request, timeout=self.node.timeout)
                    break
                except Exception as e:
                    print(f"Failure during write operation: {e}")  # Continue to next in chain.
        entry.dirty = False  # mark entry ad non-dirty, change has been propagated in chain
        return bookshop_pb2.WriteResponse(book=entry)

    def Read(self, request, context):
        books = [self.books[book] for book in request.books if book in self.books]
        dirty_books = [book for book in books if book.dirty]
        if dirty_books and not request.allow_dirty:
            # we have dirty reads, checking with tail to get fully committed changes for dirty reads
            tail = self.node.chain[-1]
            request = bookshop_pb2.ReadRequest(books=dirty_books, allow_dirty=False)
            with grpc.insecure_channel(tail.address) as channel:
                stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
                response = stub.Read(request, timeout=self.node.timeout)
            books = [book for book in books if not book.dirty] + response.books
        return bookshop_pb2.ReadResponse(books=books)

    def List(self, request, context):
        request = bookshop_pb2.ReadRequest(books=self.books.keys(), allow_dirty=request.allow_dirty)
        response = self.Read(request, context)
        return bookshop_pb2.ListResponse(books=response.books)

    def RestoreHead(self, request, context):
        successful = False
        tail = self.node.chain[-1]
        request = bookshop_pb2.ListRequest(allow_dirty=False)
        with grpc.insecure_channel(tail.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            try:
                response = stub.List(request, timeout=self.node.timeout)
                self.books = {book.book: book for book in response.books}
                successful = True
            except Exception as e:
                print(f"Failure during restore operation: {e}")

        return bookshop_pb2.RestoreHeadResponse(successful=successful)

    def tail(self):
        tail = False
        for datastore in self.node.chain:
            if tail:
                yield datastore
            elif datastore.address == self.address:
                tail = True
