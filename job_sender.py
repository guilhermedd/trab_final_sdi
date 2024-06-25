import pika
import configparser
from time import sleep
import numpy as np
import json
from datetime import datetime
from job import Job
import argparse


def main(num_msg):
    config = configparser.ConfigParser()
    config.read('config.properties')

    QUEUE = config.get('RABBITMQ', 'QUEUE')
    HOST = config.get('RABBITMQ', 'HOST')

    # Conexão com o RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE) # Declara a fila se ela ainda não existir

    try:
        with open('last_id.bin', 'rb') as f:
            id = int(f.read())
    except ValueError as e:
        id = 0
    
    if num_msg == -1:
        num_msg = np.random.poisson(100, 1)[0]


    while num_msg > 0:
        sleeping_time = np.random.poisson(5, 1)[0]   # Poisson filter
        sleep(0.1)

        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d-%H:%M:%S")

        job = Job()
        job.generate_self(id, formatted_time)

        try:
            msg = json.dumps(job.to_dict()) # Converte o objeto Job para um dicionário e depois para JSON
            
            channel.basic_publish(exchange='', routing_key=QUEUE, body=msg) # Envia a mensagem para a fila
            print(f"[x] Mensagem enviada: {job.to_dict()}")
            
            with open('last_id.bin', 'wb') as f:
                id += 1
                f.write(bytes(str(id), 'utf-8'))

        except Exception as e:
            print(f"Erro ao enviar mensagem: {e}")
            break
        num_msg -= 1
    
    connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enviar mensagens para RabbitMQ.')
    parser.add_argument('num_msg', type=int, help='Número de mensagens a serem enviadas')
    args = parser.parse_args()
    main(args.num_msg)
