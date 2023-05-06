import etcd3
from data_store import DataStore
import json

class EtcdService:

    def __init__(self, etcd_host='localhost', etcd_port=2379):
        self.etcd = etcd3.client(host=etcd_host, port=etcd_port)
        self.chain_state_key = '/bookshop/chain_state'

    def register_node(self, node_id, node_address):
        self.etcd.put(f'/bookshop/nodes/{node_id}', node_address)

    def discover_nodes(self):
        nodes = {}
        for value, metadata in self.etcd.get_prefix('/bookshop/nodes/'):
            node_id = metadata.key.decode().split('/')[-1]
            nodes[node_id] = value.decode()
        return nodes

    def clean_up(self):
        self.etcd.delete_prefix('/bookshop/nodes/')

    def set_chain_state(self, processes, head, tail):
        state = {
            "processes": [process.uuid for process in processes],
            "head": head.uuid if head else None,
            "tail": tail.uuid if tail else None,
        }
        self.etcd.put(self.chain_state_key, json.dumps(state))

    def get_chain_state(self):
        state_str, _ = self.etcd.get(self.chain_state_key)
        if state_str:
            state = json.loads(state_str)
            processes = [DataStore(uuid.split("-")[0], uuid.split("-")[1]) for uuid in state["processes"]]
            head = next((process for process in processes if process.uuid == state["head"]), None)
            tail = next((process for process in processes if process.uuid == state["tail"]), None)
            return processes, head, tail
        else:
            return [], None, None

    def update_chain(self, processes):
        head = processes[0] if processes else None
        tail = processes[-1] if processes else None
        self.set_chain_state(processes, head, tail)
