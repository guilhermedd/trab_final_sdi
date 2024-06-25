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
        self.current_running_jobs = []
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
                continue  # Se a configuração falhar, continue tentando

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

    def process_messages(self):
        while self.running:
            try:
                with self.lock:
                    while not self.message_queue.empty():
                        msg, channel, delivery_tag = self.message_queue.get()
                        resources = int(msg['resources'])

                        if self.cur_res + resources <= self.max_res:
                            self.cur_res += resources
                            self.current_running_jobs.append([msg, channel, delivery_tag, 0])
                        else:
                            self.message_queue.put((msg, channel, delivery_tag))
            except Exception as e:
                print(f"{TerminalColors.RED}Erro ao processar mensagem: {e}{TerminalColors.END}")

            for job in self.current_running_jobs[:]:  # Use [:] para iterar sobre uma cópia da lista
                try:
                    if job[3] >= int(job[0]['processing_time']):
                        job[1].basic_ack(delivery_tag=job[2])
                        self.cur_res -= int(job[0]['resources'])
                        self.current_running_jobs.remove(job)
                        print(f"{TerminalColors.GREEN}Término do processamento da mensagem {job[0]['id']} | Tempo: {datetime.datetime.now()}{TerminalColors.END}")
                    else:
                        job[3] += 1
                except pika.exceptions.AMQPError as e:
                    print(f"{TerminalColors.RED}Erro ao enviar ACK para mensagem {job[0]['id']}: {e}{TerminalColors.END}")

                time.sleep(1)

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
