import configparser
import socket
import threading
import pika
import json
import time
from datetime import datetime
from queue import Queue
from job import Job  # Certifique-se de importar sua classe Job corretamente

class Scheduler:
    def __init__(self):
        # Carregar configurações do arquivo config.properties
        self.config = configparser.ConfigParser()
        self.config.read('config.properties')

        # Variáveis de controle de recursos inicializadas
        self.cur_res = 0
        self.election_in_progress = False
        self.coordinator = None

        # Lista de nós e configurações do algoritmo de eleição
        self.nodes = []
        self.bully_id = None
        self.max_resources = int(self.config.get('SCHEDULER', 'MAX_RES'))
        print("Max resources:", self.max_resources)

        # Configurações de rede (IP e porta)
        self.port = int(self.config.get('TCP', 'PORT'))
        self.ip = self.config.get('TCP', 'IP')

        # Configurações RabbitMQ (host e fila)
        self.host = self.config.get('RABBITMQ', 'HOST')
        self.queue = self.config.get('RABBITMQ', 'QUEUE')

        # Fila para armazenar mensagens recebidas
        self.message_queue = Queue()

        # Conjunto para rastrear IDs de jobs em processamento
        self.processing_jobs = set()

        # Trava para acesso thread-safe à capacidade
        self.capacity_lock = threading.Lock()

        # Configuração do servidor de soquete (não usado no exemplo atual)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Conexão e canal RabbitMQ
        self.connection = None
        self.channel = None

        # Sinalizador para controlar a execução do consumo de mensagens
        self.running = True

    def setup_rabbitmq(self):
        # Estabelecer conexão com o servidor RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue)
        print("Iniciando consumo de mensagens")

        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)

    def start_consuming(self):
        while self.running:
            self.connection.process_data_events(time_limit=1)  # Processar eventos por até 1 segundo

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            print(f"Mensagem recebida: {message['id']} | {message['processing_time']}, {datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}")
            current_time = datetime.now()
            formatted_time = current_time.strftime("%Y-%m-%d-%H:%M:%S")
            job = Job(
                id=int(message['id']),
                processing_time=int(message['processing_time']),
                arrival_time=formatted_time,
                resources=int(message['resources']),
                send_time=message['send_time']
            )
            self.message_queue.put(job)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
        except Exception as e:
            print(f"Erro no callback: {e}")

    def process_messages(self, func=lambda message: message):
        while self.running:
            with self.capacity_lock:
                sorted_queue = sorted(list(self.message_queue.queue), key=func)
                if sorted_queue:
                    print("Queue: ",[x.id for x in sorted_queue], datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
                for message in sorted_queue:
                    if self.cur_res + message.resources <= self.max_resources and message.id not in self.processing_jobs:
                        self.start_message_processing(message)
                        break
            time.sleep(0.1)  # Dormir para evitar espera ativa

    def start_message_processing(self, message):
        process_msg_thread = threading.Thread(target=self.process_message, args=(message,))
        process_msg_thread.start()

    def process_message(self, message):
        try:
            with self.capacity_lock:
                if self.cur_res + message.resources > self.max_resources:
                    return

                self.cur_res += message.resources
            self.processing_jobs.add(message.id)  # Adicionar ID do job ao conjunto de jobs em processamento

            current_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            message.start_time = current_time
            print(f"Processando mensagem {message.id}: dormindo por {message.processing_time} segundos, recursos atuais utilizados: {self.cur_res}, {datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}")
            time.sleep(message.processing_time)

            with self.capacity_lock:
                self.cur_res -= message.resources

            current_time = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            message.end_time = current_time
            message.done = True
            print(f"Mensagem processada: {message.id}, {datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}")
            self.remove_message(message)

        except Exception as e:
            print(f"Erro ao processar mensagem {message.id}: {e}")
            # Aqui você pode adicionar lógica para lidar com o erro, como reiniciar o processamento ou registrar a exceção
 


    def remove_message(self, message):
        with self.capacity_lock:
            try:
                self.message_queue.queue.remove(message)
                self.processing_jobs.remove(message.id)
            except ValueError:
                print(f"O job {message.id} não está na fila")

    def start(self):
        # Configurar RabbitMQ
        self.setup_rabbitmq()

        # Iniciar threads para consumir e processar mensagens
        consumer_thread = threading.Thread(target=self.start_consuming)
        FUNCTION = lambda msg: msg.processing_time
        processor_thread = threading.Thread(target=self.process_messages, args=(FUNCTION,))

        consumer_thread.start()
        processor_thread.start()

        try:
            consumer_thread.join()
            processor_thread.join()
        except KeyboardInterrupt:
            print("Interrupção do escalonador...")
            self.running = False  # Definir sinalizador para parar o consumo de mensagens
            self.connection.close()
            consumer_thread.join()
            processor_thread.join()

if __name__ == "__main__":
    # Criar e iniciar uma instância do Scheduler
    scheduler = Scheduler()
    scheduler.start()
