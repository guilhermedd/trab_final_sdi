import configparser
import socket
import threading

class Scheduler:
    def __init__(self):
        self.config = configparser.ConfigParser()

        self.resources = None
        self.bully_id = None
        self.nodes = []
        self.election_in_progress = False
        self.coordinator = None
        self.config.read('config.properties')
        self.resources = int(self.config.get('SCHEDULER', 'MAX_RES'))

        self.port = self.config.get('TCP', 'PORT')
        self.ip = self.config.get('TCP', 'IP')
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        for con in self.nodes:
            threading.Thread(target=self.listen_for_connection, args=(con,)).start()

    def create_self(self, id):
        self.bully_id = id
        self.coordinator = id
    
    def add_node(self, node_id, ):
        if node_id not in self.nodes:
            self.nodes.append(node_id)
    
    def start_election(self):
        self.election_in_progress = True
        print(f"Nó {self.bully_id} iniciou uma eleição.")
        
        higher_nodes = [node for node in self.nodes if node > self.bully_id]
        if not higher_nodes:
            self.declare_victory()
        else:
            for node in higher_nodes:
                self.send_election_message(node)
    
    def send_election_message(self, node_id):
        print(f"Nó {self.bully_id} envia mensagem de eleição para nó {node_id}.")
        # Simulação de recebimento de mensagem de resposta
        self.receive_ok_message(node_id)
    
    def receive_ok_message(self, node_id):
        print(f"Nó {self.bully_id} recebeu OK de nó {node_id}.")
        self.election_in_progress = False
        # Simular nó maior iniciando sua própria eleição
        self.handle_election_message(node_id)
    
    def handle_election_message(self, node_id):
        print(f"Nó {node_id} está iniciando sua própria eleição.")
        # Se este nó é maior, ele se declara vencedor
        if node_id > self.bully_id:
            self.bully_id = node_id
            self.declare_victory()
    
    def declare_victory(self):
        self.coordinator = self.bully_id
        self.election_in_progress = False
        print(f"Nó {self.bully_id} se declara vencedor e coordenador.")
        for node in self.nodes:
            if node != self.bully_id:
                self.send_victory_message(node)
    
    def send_victory_message(self, node_id):
        print(f"Nó {self.bully_id} envia mensagem de vitória para nó {node_id}.")

    def listen_for_connection(self, connection):
        self.server.bind(self.ip, self.port)