class DataStore:
    def __init__(self, node_id, process_id):
        self.process_id = process_id
        self.uuid = f"{node_id}-{process_id}"
        self.books = {}
        self.predecessor = None
        self.successor = None