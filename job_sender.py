import pika
import configparser
from time import sleep
import numpy as np
import json
from datetime import datetime

from job import Job

config = configparser.ConfigParser()
config.read('config.properties')

# Acessa os valores do arquivo de configuração
QUEUE = config.get('RABBITMQ', 'QUEUE')
HOST = config.get('RABBITMQ', 'HOST')
KEY = config.get('RABBITMQ', 'ROUTING_KEY')

# Conexão com o RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))
channel = connection.channel()

# Declara a fila se ela ainda não existir
channel.queue_declare(queue=QUEUE)

i = 0


while True:
    sleep(np.random.poisson(5, 1)[0])  # Poisson filter

    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d-%H:%M:%S")

    job = Job()
    job.generate_self(i, formatted_time)

    try:
        # Converte o objeto Job para um dicionário e depois para JSON
        msg = json.dumps(job.to_dict())
        
        # Envia a mensagem para a fila
        channel.basic_publish(exchange='', routing_key=KEY, body=msg)
        
        print(f"[x] Mensagem enviada: {job.to_dict()}")  # Ajuste na impressão da mensagem enviada
        i += 1

    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")
        break

# Fecha a conexão ao finalizar o loop
connection.close()
