import configparser
import json
import threading
import datetime
import time
import pika
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

class TerminalColors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

class Scheduler:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.properties')

        self.cur_res = 0
        self.max_res = int(self.config.get('SCHEDULER', 'MAX_RES'))
        print("Max resources:", self.max_res)

        self.host = self.config.get('RABBITMQ', 'HOST')
        self.queue = self.config.get('RABBITMQ', 'QUEUE')

        self.message_queue = Queue()
        self.running = True
        self.connection = None
        self.channel = None
        self.executor = ThreadPoolExecutor(max_workers=int(self.max_res/10))
        self.lock = threading.Lock()

    def setup_rabbitmq(self):
        while self.running:
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
        while self.running:
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
        while self.running:
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
        self.running = False
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"{TerminalColors.RED}Erro ao fechar conexão: {e}{TerminalColors.END}")

if __name__ == "__main__":
    scheduler = Scheduler()
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.stop()
