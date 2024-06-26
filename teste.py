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
import signal

class TerminalColors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

class Scheduler:
    def __init__(self, semi_bully_order, is_master=False):
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
        self.consensus = int(self.config.get('TCP', 'PORT_CONSENSUS'))
        self.semi_bully_order = semi_bully_order
        self.my_port = self.ports[semi_bully_order - 1][0]  # Seleciona a porta correta do semi_bully_order
        self.cur_master = 1
        self.master_alive = False

        self.shut_all = False

        # Configurando o tratamento de sinais
        signal.signal(signal.SIGINT, self.handle_force_quit)

    def handle_force_quit(self, signum, frame):
        print(f"\nReceived SIGINT (Ctrl+C). Shutting down gracefully...")

    # TCP CON
    def get_status(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.ip, self.my_port))
                s.listen()
                print(f'Servidor ouvindo em {self.ip}:{self.my_port}')
                while True:
                    conn, addr = s.accept()
                    threading.Thread(target=self.handle_client, args=(conn, addr)).start()
                    if self.shut_all:
                        conn.close()
                        break
        except Exception as e:
            print(f"{TerminalColors.RED}Erro ao conectar com consenso: {e}{TerminalColors.END}")
            with self.lock:
                self.is_master = False

    def handle_client(self, conn, addr):
        print(f'Nova conexão de {addr}')
        with conn:
            while True:
                if self.shut_all:
                    conn.close()
                    return None
                data = conn.recv(1024)
                if not data:
                    break
                if data.decode('utf-8') == 'YOU_ARE_MASTER':
                    with self.lock:
                        self.is_master = True
                print(f'Mensagem recebida de {addr}: {data.decode()}')
                confirmation_message = 'ACK'
                conn.sendall(confirmation_message.encode())
        print(f'Conexão encerrada de {addr}')
            
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
            if self.shut_all:
                return self.stop()

    def start_consuming(self):
        while True:
            if not self.setup_rabbitmq() or not self.is_master:
                continue  # If setup fails, retry

            try:
                self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
                self.channel.start_consuming()
            except Exception as e:
                print(f"{TerminalColors.RED}Erro ao consumir mensagens: {e}{TerminalColors.END}")
                if self.connection and self.connection.is_open:
                    self.connection.close()
                time.sleep(1)  # Espera antes de tentar novamente
                if self.shut_all:
                    return self.stop()

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            print(f"Mensagem recebida: {message['id']} | processing_time: {message['processing_time']} | resources: {message['resources']}")

            with self.lock:
                self.message_queue.put((message, ch, method.delivery_tag))
                if not self.is_master:
                    raise Exception("We are no longer master")

        except Exception as e:
            print(f"{TerminalColors.RED}Erro no callback: {e}{TerminalColors.END}")

        time.sleep(1)

    def reject_message(self, ch, delivery_tag):
        try:
            if self.channel and self.channel.is_open:
                ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
            else:
                print(f"{TerminalColors.RED}Erro ao rejeitar mensagem: Canal fechado.{TerminalColors.END}")
                return self.stop()
        except pika.exceptions.AMQPError as nack_error:
            print(f"{TerminalColors.RED}Erro ao rejeitar mensagem: {nack_error}{TerminalColors.END}")
            return self.stop()  

    def process_messages(self):
        print(f'{TerminalColors.BLUE}Processando mensagens...{TerminalColors.END}')
        while True:
            if not self.is_master:
                continue

            if self.shut_all:
                return

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
                    return self.reject_message(channel, delivery_tag)

            time.sleep(1)

    def process_job(self, message, channel, delivery_tag):
        while self.is_master:
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
                    return self.reject_message(channel, delivery_tag)

            except pika.exceptions.AMQPError as e:
                print(f"{TerminalColors.RED}Erro no processamento da mensagem {message['id']}: {e}{TerminalColors.END}")
            except Exception as e:
                print(f"{TerminalColors.RED}Erro no processamento da mensagem {message['id']}: {e}{TerminalColors.END}")
            
    def start(self):
        process_job_thread = threading.Thread(target=self.process_messages)
        process_job_thread.start()

        status_thread = threading.Thread(target=self.get_status)
        status_thread.start()

        self.start_consuming()

    def stop(self):
        self.shut_all = True
        self.is_master = False
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"{TerminalColors.RED}Erro ao fechar conexão: {e}{TerminalColors.END}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indice do .')
    parser.add_argument('num_idx', type=int, help='Indice do bully')
    args = parser.parse_args()
    master = args.num_idx == 3
    scheduler = Scheduler(int(args.num_idx))
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Stopping scheduler")
        scheduler.stop()
