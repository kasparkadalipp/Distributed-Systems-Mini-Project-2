import sys
import threading

import protocol_pb2
import protocol_pb2_grpc


class Node(protocol_pb2_grpc.BookStoreServiceServicer):
    def __init__(self, node_id, port):
        self.dataStoreList = []
        self.node_id = node_id
        print(f"Starting node '{node_id}', listening on '{port}'")
        # TODO start node

    def accept_user_input(self):
        user_input = input().strip()
        match user_input:
            case 'Create-chain':
                pass  # TODO
            case 'List-chain':
                pass  # TODO
            case 'List-books':
                pass  # TODO
            case 'Read-operation':
                pass  # TODO
            case 'Data-status':
                pass  # TODO
            case 'Remove-head':
                pass  # TODO
            case 'Restore-head':
                pass  # TODO

        if user_input.startswith('Local-store-ps'):  # <k processes>
            k_processes = int(user_input[14:])
            self.dataStoreList = (DataStore(self.node_id, process_id) for process_id in range(1, k_processes+1))
        elif user_input.startswith('Write-operation'):  # <Book, Price>
            name, price = user_input[15:].split(',')
            price = float(price)
            # TODO
        elif user_input.startswith('Time-out'):  # <seconds>
            seconds = int(user_input[8:])
            # TODO
        elif user_input.startswith('ML-list-recommend'):  # <prompt>
            prompt = user_input[17:].strip()
            # TODO


class DataStore():
    def __init__(self, node_id, process_id):
        self.name = f"Node{node_id}-ps{id}"

def main():
    if not len(sys.argv) == 2:
        print("Provide node id")
        sys.exit(1)
    node_id = int(sys.argv[1])
    node_port = 5000 + node_id

    node = Node(node_id, node_port)
    while True:
        try:
            node.accept_user_input()
        except KeyboardInterrupt:
            print("Exiting...")
            break


main()
