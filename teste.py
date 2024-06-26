import configparser
import json
import threading
import datetime
import time
import pika
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import socket
import argparse

class TerminalColors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

class Scheduler:
    def __init__(self, bully_order, is_master=False):
        self.config = configparser.ConfigParser()
        self.config.read('config.properties')

        self.cur_res = 0
        self.max_res = int(self.config.get('SCHEDULER', 'MAX_RES'))
        print("Max resources:", self.max_res)

        self.host = self.config.get('RABBITMQ', 'HOST')
        self.queue = self.config.get('RABBITMQ', 'QUEUE')

        self.message_queue = Queue()
        self.is_master = is_master
        self.connection = None
        self.channel = None
        self.executor = ThreadPoolExecutor(max_workers=int(self.max_res/10))
        self.lock = threading.Lock()

        # TCP
        self.ip = self.config.get('TCP', 'IP')
        self.ports = (
            (int(self.config.get('TCP', 'PORT_1')), 3),
            (int(self.config.get('TCP', 'PORT_2')), 2),
            (int(self.config.get('TCP', 'PORT_3')), 1),
        )
        self.bully_order = bully_order
        self.my_port = self.ports[bully_order - 1][0]  # Seleciona a porta correta do bully_order
        self.cur_master = 1
        self.minions_alive = {port: True for port, _ in self.ports if port != self.my_port}

        self.server_thread = threading.Thread(target=self.listen_to_master)
        self.server_thread.start()
        
        self.communication_thread = threading.Thread(target=self.communicate_to_minions)
        self.communication_thread.start()

    # TCP CON
    def communicate_to_minions(self):
        while True:
            errors = 0
            for port, _ in self.minions_alive.items():
                try:
                    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_socket.connect((self.ip, port))
                    msg = bytes("IM_MASTER", 'utf-8')
                    tcp_socket.sendall(msg)
                    tcp_socket.close()
                except socket.error as e:
                    errors += 1
                    print(f'Error communicating with minion at port {port}: {e}')
                    self.minions_alive[port] = False
            
            if errors == len(self.minions_alive):
                self.is_master = False
                print("Master is down.")
            else:
                self.is_master = True
            
            time.sleep(5)  # Tempo de espera entre verificações
    
    def listen_to_master(self):
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.ip, self.my_port))
            server_socket.listen(len(self.ports) - 1)
            print(f'Server listening on {self.ip}:{self.my_port}')
            
            while True:
                client_socket, client_address = server_socket.accept()
                print(f'Accepted connection from {client_address}')
                
                data = client_socket.recv(1024)
                if not data:
                    break
                
                message = data.decode('utf-8').strip()
                if message == 'IM_MASTER':
                    print(f'Received IM_MASTER message from {client_address}')
                    self.is_master = True
                
                response = 'ACK'
                client_socket.sendall(response.encode('utf-8'))
                
                client_socket.close()
                
        except Exception as e:
            print(f'Server error: {e}')
        finally:
            server_socket.close()
    
    def communicate(self):
        while True:
            if not self.is_master:
                # Lógica para o minion se tornar o novo mestre
                highest_order_minion = max(self.ports, key=lambda x: x[1])
                if highest_order_minion[1] > self.bully_order and not self.is_master:
                    print(f"I'm becoming the new master. Informing other minions.")
                    self.is_master = True
                    # Lógica para informar aos outros minions que sou o novo mestre
                    for port in self.minions_alive:
                        if port != self.my_port:
                            try:
                                tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                tcp_socket.connect((self.ip, port))
                                msg = bytes(f"NEW_MASTER {self.my_port}", 'utf-8')
                                tcp_socket.sendall(msg)
                                tcp_socket.close()
                            except socket.error as e:
                                print(f'Error communicating with minion at port {port}: {e}')
            
            time.sleep(5)  # Intervalo entre as verificações

    def setup_rabbitmq(self):
        while self.is_master:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue)
                print(f"{TerminalColors.GREEN}Iniciando consumo de mensagens{TerminalColors.END}")
                return True
            except pika.exceptions.AMQPError as e:
                print(f"{TerminalColors.RED}Erro ao configurar RabbitMQ: {e}{TerminalColors.END}")
                if self.connection and self.connection.is_open:
                    self.connection.close()
                time.sleep(1)  # Espera antes de tentar novamente
        return False

    def start_consuming(self):
        while self.is_master:
            if not self.setup_rabbitmq():
                continue  # If setup fails, retry

            try:
                self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
                self.channel.start_consuming()
            except pika.exceptions.AMQPError as e:
                print(f"{TerminalColors.RED}Erro ao consumir mensagens: {e}{TerminalColors.END}")
                if self.connection and self.connection.is_open:
                    self.connection.close()
                time.sleep(1)  # Espera antes de tentar novamente

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            print(f"Mensagem recebida: {message['id']} | processing_time: {message['processing_time']} | resources: {message['resources']}")

            with self.lock:
                self.message_queue.put((message, ch, method.delivery_tag))

        except Exception as e:
            print(f"{TerminalColors.RED}Erro no callback: {e}{TerminalColors.END}")

        time.sleep(1)

    def reject_message(self, ch, delivery_tag):
        try:
            if self.channel and self.channel.is_open:
                ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
            else:
                print(f"{TerminalColors.RED}Erro ao rejeitar mensagem: Canal fechado.{TerminalColors.END}")
        except pika.exceptions.AMQPError as nack_error:
            print(f"{TerminalColors.RED}Erro ao rejeitar mensagem: {nack_error}{TerminalColors.END}")

    def process_messages(self):
        print(f'{TerminalColors.BLUE}Processando mensagens...{TerminalColors.END}')
        while self.is_master:
            temp_list = []

            while not self.message_queue.empty():
                with self.lock:
                    msg, channel, delivery_tag = self.message_queue.get()
                resources = int(msg['resources'])

                with self.lock:
                    if self.cur_res + resources <= self.max_res:
                        self.cur_res += resources
                        temp_list.append((msg, channel, delivery_tag))
                    else:
                        print(f"{TerminalColors.YELLOW}Recursos insuficientes para processar mensagem {msg['id']} | Requested {resources} | Available {self.max_res - self.cur_res}{TerminalColors.END}")

            sorted_temp_list = sorted(temp_list, key=lambda x: x[0]['processing_time'])

            for msg, channel, delivery_tag in sorted_temp_list:
                try:
                    self.executor.submit(self.process_job, msg, channel, delivery_tag)
                except Exception as e:
                    print(f"{TerminalColors.RED}Erro no processamento da mensagem: {e}{TerminalColors.END}")
                    self.reject_message(channel, delivery_tag)

            time.sleep(1)

    def process_job(self, message, channel, delivery_tag):
        while True:
            try:
                processing_time = int(message['processing_time'])
                resources = int(message['resources'])
                print(f"{TerminalColors.BLUE}Processando mensagem {message['id']} por {processing_time} segundos | Tempo: {datetime.datetime.now()}{TerminalColors.END}")
                time.sleep(processing_time)

                retries = 3
                while retries > 0:
                    try:
                        if channel and channel.is_open:
                            with self.lock:
                                channel.basic_ack(delivery_tag=delivery_tag)
                                print(f"{TerminalColors.GREEN}Término do processamento da mensagem {message['id']} | Tempo: {datetime.datetime.now()}{TerminalColors.END}")
                                self.cur_res -= resources
                            return
                        else:
                            raise pika.exceptions.AMQPError("Canal fechado.")
                    except pika.exceptions.AMQPError as ack_error:
                        print(f"{TerminalColors.RED}Erro ao enviar ack para {message['id']}: {ack_error}{TerminalColors.END}")
                        retries -= 1
                        time.sleep(1)

                with self.lock:
                    self.cur_res -= resources
                    self.reject_message(channel, delivery_tag)
                    return

            except pika.exceptions.AMQPError as e:
                print(f"{TerminalColors.RED}Erro no processamento da mensagem {message['id']}: {e}{TerminalColors.END}")
            except Exception as e:
                print(f"{TerminalColors.RED}Erro no processamento da mensagem {message['id']}: {e}{TerminalColors.END}")
            
    def start(self):
        process_job_thread = threading.Thread(target=self.process_messages)
        process_job_thread.start()

        self.start_consuming()

    def stop(self):
        self.is_master = False
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"{TerminalColors.RED}Erro ao fechar conexão: {e}{TerminalColors.END}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indice do bully.')
    parser.add_argument('num_idx', type=int, help='Indice do bully')
    args = parser.parse_args()
    scheduler = Scheduler(int(args.num_idx), args == 3)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.stop()
