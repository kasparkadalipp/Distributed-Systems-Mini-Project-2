import protocol_pb2
import protocol_pb2_grpc


class Node(protocol_pb2_grpc.BookStoreServiceServicer):

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
            # TODO
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


def main():
    node = Node()

    while True:
        try:
            node.accept_user_input()
        except KeyboardInterrupt | SystemExit:
            print("Exiting...")
            break


main()
