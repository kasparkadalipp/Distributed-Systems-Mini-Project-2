import etcd3
import grpc

from etcd_service import Etcd, get_host_ip
import unittest
import bookshop_pb2
import bookshop_pb2_grpc
import time
from book_node import Node
from datastore import DataStore


class TestEtcd(unittest.TestCase):

    def setUp(self):
        etcd_host = "localhost"  # Change this to your etcd host
        etcd_port = 2379  # Change this to your etcd port
        self.etcd = Etcd(etcd_host, etcd_port)

    def tearDown(self):
        self.etcd.etcd.delete_prefix('/nodes')
        self.etcd.etcd.delete_prefix('/datastores')
        self.etcd.etcd.delete_prefix('/node_counter')

    def test_generate_node_id(self):
        node_id1 = self.etcd.generate_node_id()
        node_id2 = self.etcd.generate_node_id()
        self.assertEqual(node_id2, node_id1 + 1)

    def test_register_node(self):
        node_id = 1
        address = f"{get_host_ip()}:8080"
        self.etcd.register_node(node_id, address)
        stored_address = str(self.etcd.etcd.get(f"/nodes/{node_id}")[0].decode())
        self.assertEqual(stored_address, address)

    def test_register_datastore(self):
        node_id = 1
        process_id = 1
        address = f"{get_host_ip()}:8081"
        self.etcd.register_datastore(node_id, process_id, address)
        stored_address = self.etcd.etcd.get(f"/datastores/{node_id}/{process_id}")[0]
        self.assertEqual(str(stored_address.decode()), address)

    def test_nodes(self):
        node_id1 = 1
        node_id2 = 2
        address1 = f"{get_host_ip()}:8080"
        address2 = f"{get_host_ip()}:8081"
        self.etcd.register_node(node_id1, address1)
        self.etcd.register_node(node_id2, address2)
        nodes = list(self.etcd.nodes())
        self.assertEqual(len(nodes), 2)
        self.assertIn((node_id1, address1), nodes)
        self.assertIn((node_id2, address2), nodes)

    def test_datastores(self):
        node_id = 1
        process_id1 = 1
        process_id2 = 2
        address1 = f"{get_host_ip()}:8080"
        address2 = f"{get_host_ip()}:8081"
        self.etcd.register_datastore(node_id, process_id1, address1)
        self.etcd.register_datastore(node_id, process_id2, address2)
        datastores = list(self.etcd.datastores())
        self.assertEqual(len(datastores), 2)
        self.assertIn(bookshop_pb2.Datastore(node_id=node_id, process_id=process_id1, address=address1), datastores)
        self.assertIn(bookshop_pb2.Datastore(node_id=node_id, process_id=process_id2, address=address2), datastores)


class TestDataStore(unittest.TestCase):

    def setUp(self):
        etcd_host = "localhost"  # Change this to your etcd host
        etcd_port = 2379  # Change this to your etcd port
        self.etcd = Etcd(etcd_host, etcd_port)
        self.node = Node(50051)
        self.node.local_store_ps(['local-store-ps', '3'])

    def tearDown(self):
        self.node.datastores = None
        self.etcd.etcd.delete_prefix('/nodes')
        self.etcd.etcd.delete_prefix('/datastores')
        self.etcd.etcd.delete_prefix('/node_counter')

    def test_write_and_read(self):
        book = "Test Book"
        price = 10
        write_request = bookshop_pb2.WriteRequest(book=book, price=price)
        write_response = self.node.datastores[0].Write(write_request, None)

        self.assertEqual(write_response.book.book, book)
        self.assertEqual(write_response.book.price, price)
        self.assertFalse(write_response.book.dirty)

        read_request = bookshop_pb2.ReadRequest(books=[book], allow_dirty=False)
        read_response = self.node.datastores[0].Read(read_request, None)

        self.assertEqual(len(read_response.books), 1)
        self.assertEqual(read_response.books[0].book, book)
        self.assertEqual(read_response.books[0].price, price)
        self.assertFalse(read_response.books[0].dirty)

    def test_list(self):
        book1 = "Test Book 1"
        book2 = "Test Book 2"
        price1 = 10
        price2 = 11
        write_request1 = bookshop_pb2.WriteRequest(book=book1, price=price1)
        write_request2 = bookshop_pb2.WriteRequest(book=book2, price=price2)
        self.node.datastores[0].Write(write_request1, None)
        self.node.datastores[0].Write(write_request2, None)

        list_request = bookshop_pb2.ListRequest(allow_dirty=False)
        list_response = self.node.datastores[0].List(list_request, None)

        self.assertEqual(len(list_response.books), 2)
        books_prices = {book.book: book.price for book in list_response.books}
        self.assertEqual(books_prices[book1], price1)
        self.assertEqual(books_prices[book2], price2)


def wait_for_condition(condition_func, timeout=10, interval=0.1):
    """
    Waits for a condition to be met or until a timeout is reached.

    :param condition_func: A function that returns a boolean value.
    :param timeout: The maximum amount of time to wait in seconds. Defaults to 10 seconds.
    :param interval: The interval between condition checks in seconds. Defaults to 0.1 seconds.
    :return: True if the condition is met, False if the timeout is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        time.sleep(interval)
    return False


class TestNode(unittest.TestCase):

    def setUp(self):
        # etcd3.Etcd3Client(host='localhost', port=2379).delete('/')
        # etcd3.Etcd3Client(host='localhost', port=2379).delete_prefix('/datastores')
        # etcd3.Etcd3Client(host='localhost', port=2379).delete_prefix('/node_counter')
        time.sleep(5)
        etcd_host = "localhost"
        etcd_port = 2379
        self.etcd = Etcd(etcd_host, etcd_port)
        self.node = Node(50051)
        self.node2 = Node(50052)

    def tearDown(self):
        self.node.server.stop(0)
        self.node2.server.stop(0)
        self.etcd.etcd.delete_prefix('/nodes')
        self.etcd.etcd.delete_prefix('/datastores')
        self.etcd.etcd.delete_prefix('/node_counter')
        self.node.datastores = None
        self.node.chain = None
        self.node2.datastores = None
        self.node2.chain = None
        self.node = None
        self.node2 = None
        time.sleep(5)

    def test_broadcast_chain(self):
        self.node.local_store_ps(['local-store-ps', '3'])
        self.node.create_chain(['create-chain'])
        self.assertIsNotNone(self.node.chain)
        self.assertEqual(len(self.node2.chain), 3)

    def test_two_node_local_store_ps(self):
        self.node.local_store_ps(['local-store-ps', '3'])
        self.node2.local_store_ps(['local-store-ps', '3'])
        self.assertEqual(len(self.node.datastores), 3)
        self.assertEqual(len(self.node2.datastores), 3)
        self.node.create_chain(['create-chain'])
        self.assertEqual(len(self.node.chain), 6)

    def test_force_recreate_chain(self):
        self.node.local_store_ps(['local-store-ps', '3'])
        self.node.create_chain(['create-chain'])
        head = self.node.chain[0]

        condition_met = wait_for_condition(lambda: self.node.chain is not None and len(self.node2.chain) == 3,
                                           timeout=10)
        self.assertTrue(condition_met, "Chain not created on node within the timeout period.")
        self.assertIsNotNone(self.node.chain)
        self.assertEqual(len(self.node2.chain), 3)

        self.node2.local_store_ps(['local-store-ps', '3'])
        self.node.create_chain(['create-chain', 'force'])
        self.assertEqual(len(self.node.chain), 6)
        self.assertNotEqual(head, self.node.chain[0])
        self.assertIsNotNone(self.node.chain)

    def test_list_books(self):
        self.node.local_store_ps(['local-store-ps', '3'])
        self.node.create_chain(['create-chain', 'force'])

        book = "Test Book"
        price = 10
        write_request = bookshop_pb2.WriteRequest(book=book, price=price)
        node2head_datastore = self.node2.chain[0]
        with grpc.insecure_channel(node2head_datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            stub.Write(write_request, timeout=self.node2.timeout)

        books = self.node.list_books([])
        self.assertIn("Test Book", books)
        self.assertIn("10", books)

    def test_data_status(self):

        self.node.local_store_ps(['local-store-ps', '3'])
        self.node2.local_store_ps(['local-store-ps', '5'])
        self.node.create_chain(['create-chain'])
        self.assertIsNotNone(self.node2.chain)
        self.assertEqual(len(self.node2.chain), 8)
        time.sleep(5)

        data_status = self.node.data_status([])
        self.assertEqual("No books in stores", data_status)

        data_status2 = self.node2.data_status([])
        self.assertEqual("No books in stores", data_status2)
        book = "Test Book"
        price = 10
        write_request = bookshop_pb2.WriteRequest(book=book, price=price)
        head_datastore = self.node2.chain[0]
        with grpc.insecure_channel(head_datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            stub.Write(write_request, timeout=self.node.timeout)
        data_status = self.node.data_status([])
        data_status2 = self.node2.data_status([])
        self.assertEqual("1) Test Book = 10", data_status)
        self.assertEqual("1) Test Book = 10", data_status2)

    def test_remove_head(self):
        self.node.local_store_ps(['local-store-ps', '3'])

        self.node.create_chain(['create-chain', 'force'])

        self.node.remove_head([])

        self.assertEqual(len(self.node.removed_heads), 1)
        self.assertEqual(len(self.node2.chain), 2)

    def test_restore_head(self):
        self.node.local_store_ps(['local-store-ps', '3'])

        self.node.create_chain(['create-chain', 'force'])

        time.sleep(3)

        self.node.remove_head([])

        self.assertEqual(len(self.node.removed_heads), 1)

        time.sleep(3)

        result = self.node.restore_head([])

        # self.assertEqual(len(self.node.removed_heads), 0)
        self.assertIn("Successfully instituted", result)

    def test_read_operation(self):
        self.node.local_store_ps(['local-store-ps', '1'])
        self.node2.local_store_ps(['local-store-ps', '1'])
        time.sleep(1)
        self.node.create_chain(['create-chain', 'force'])
        time.sleep(3)

        book = "Test Book"
        price = 10
        write_request = bookshop_pb2.WriteRequest(book=book, price=price)
        head_datastore = self.node.chain[0]
        with grpc.insecure_channel(head_datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            stub.Write(write_request, timeout=self.node.timeout)
            time.sleep(3)

        read = self.node2.read_operation(['read', 'Test Book'])
        self.assertIn("Test Book", str(read))

    def test_write_operation(self):
        self.node.local_store_ps(['local-store-ps', '3'])

        self.node.create_chain(['create-chain', 'force'])

        self.node.write_operation(['write', '(Test Book, 15.0)'])

        books = self.node.list_books([])

        self.assertIn("Test Book", books)

        read_request = bookshop_pb2.ReadRequest(books=['Test Book'], allow_dirty=True)
        tail_datastore = self.node.chain[-1]
        with grpc.insecure_channel(tail_datastore.address) as channel:
            stub = bookshop_pb2_grpc.DatastoreServiceStub(channel)
            response = stub.Read(read_request, timeout=self.node.timeout)

        self.assertEqual("Test Book", response.books[0].book)

    def test_set_timeout(self):
        self.node.set_timeout(['set-timeout', '2'])

        self.assertEqual(self.node.timeout, 120)


if __name__ == "__main__":
    unittest.main()
