import configparser
import pika
import json
import threading
import time
import datetime

class Scheduler:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.properties')

        self.cur_res = 0
        self.max_res = int(self.config.get('SCHEDULER', 'MAX_RES'))
        print("Max resources:", self.max_res)

        self.host = self.config.get('RABBITMQ', 'HOST')
        self.queue = self.config.get('RABBITMQ', 'QUEUE')

        self.ids = []

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
        while self.running:
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
            if message['id'] not in self.ids:
                print(f"Mensagem recebida: {message['id']} | processing_time: {message['processing_time']} | resources: {message['resources']}")
                self.ids.append(message['id'])

            resources = int(message['resources'])
            if self.cur_res + resources <= self.max_res:
                threading.Thread(target=self.process_job, args=(message, ch, method.delivery_tag)).start()
            else:
                print(f"Recursos insuficientes para processar mensagem {message['id']} | Used resources: {self.cur_res} | req_res : {message['resources']}")
                self.reject_message(ch, method.delivery_tag, message['id'])
            
            time.sleep(0.1)

        except Exception as e:
            print(f"Erro no callback: {e}")

    def reject_message(self, channel, delivery_tag, id):
        try:
            channel.basic_reject(delivery_tag=delivery_tag, requeue=True)
            print(f"Mensagem com delivery_tag {delivery_tag} e id: {id} reenfileirada")
        except pika.exceptions.AMQPError as e:
            print(f"Erro ao rejeitar mensagem: {e}")

    def process_job(self, message, channel, delivery_tag):
        try:
            processing_time = int(message['processing_time'])
            resources = int(message['resources'])
            print(f"Processando mensagem {message['id']} por {processing_time} segundos | Tempo: {datetime.datetime.now()}")
            self.cur_res += resources
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
        self.start_consuming()

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.start()
