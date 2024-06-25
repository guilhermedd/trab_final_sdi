import configparser
import pika
import json
import threading
import time
import datetime
from queue import Queue

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

    def setup_rabbitmq(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue)
            print("Iniciando consumo de mensagens")
        except pika.exceptions.AMQPError as e:
            print(f"Erro ao configurar RabbitMQ: {e}")
            time.sleep(5)  # Espera antes de tentar reconectar

    def start_consuming(self):
        while True:
            self.setup_rabbitmq()
            self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=False)
            try:
                self.channel.start_consuming()
            except pika.exceptions.AMQPError as e:
                print(f"Erro ao iniciar consumo de mensagens: {e}")
                self.setup_rabbitmq()  # Tentar reconectar em caso de erro

    def callback(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            print(f"Mensagem recebida: {message['id']} | processing_time: {message['processing_time']} | resources: {message['resources']}")
            
            resources = int(message['resources'])
            if self.cur_res + resources <= self.max_res:
                self.message_queue.put((message, ch, method.delivery_tag))
                self.cur_res += resources
            else:
                print(f"Recursos insuficientes para processar mensagem {message['id']}.")
                self.reject_message(ch, method.delivery_tag)

        except Exception as e:
            print(f"Erro no callback: {e}")

        time.sleep(0.5)
    
    def reject_message(self, ch, delivery_tag):
        try:
            ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
        except pika.exceptions.AMQPError as nack_error:
            print(f"Erro ao rejeitar mensagem: {nack_error}")

    def process_messages(self):
        print('processando mensagens...')
        while self.running:
            temp_list = []

            while not self.message_queue.empty():
                msg, channel, delivery_tag = self.message_queue.get()
                temp_list.append((msg, channel, delivery_tag))

            sorted_temp_list = sorted(temp_list, key=lambda x: x[0]['processing_time'])
            for msg, channel, delivery_tag in sorted_temp_list:
                try:
                    self.process_job(msg, channel, delivery_tag)

                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON: {e}")
                    # Loga ou descarta a mensagem, não a adicionando de volta à fila
                except Exception as e:
                    print(f"Erro no processamento da mensagem: {e}")
                    # Adicione a mensagem de volta à fila se o delivery_tag não for None
                    if delivery_tag is not None:
                        self.message_queue.put((msg, channel, delivery_tag))

    def process_job(self, message, channel, delivery_tag):
        try:
            processing_time = int(message['processing_time'])
            resources = int(message['resources'])
            print(f"Processando mensagem {message['id']} por {processing_time} segundos | Tempo: {datetime.datetime.now()}")
            time.sleep(processing_time)

            retries = 3
            while retries > 0:
                try:
                    channel.basic_ack(delivery_tag=delivery_tag)
                    print(f"Término do processamento da mensagem {message['id']} | Tempo: {datetime.datetime.now()}")
                    self.cur_res -= resources
                    break
                except pika.exceptions.AMQPError as ack_error:
                    print(f"Erro ao enviar ack: {ack_error}")
                    retries -= 1
                    time.sleep(1)

        except pika.exceptions.AMQPError as e:
            print(f"Erro ao enviar ack: {e}")
        except Exception as e:
            print(f"Erro no processamento da mensagem: {e}")

    def start(self):
        process_job = threading.Thread(target=self.process_messages)
        process_job.start()

        self.start_consuming()

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.start()
