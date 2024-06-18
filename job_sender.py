import pika
import configparser
from time import sleep
import numpy as np
import json
from datetime import datetime
from job import Job


def main():
    config = configparser.ConfigParser()
    config.read('config.properties')

    QUEUE = config.get('RABBITMQ', 'QUEUE')
    HOST = config.get('RABBITMQ', 'HOST')
    KEY = config.get('RABBITMQ', 'ROUTING_KEY')

    # Conexão com o RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE) # Declara a fila se ela ainda não existir

    try:
        with open('last_id.bin', 'rb') as f:
            id = int(f.read())
    except ValueError as e:
        id = 0


    while True:
        sleeping_time = np.random.poisson(5, 1)[0]   # Poisson filter
        sleep(sleeping_time)

        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d-%H:%M:%S")

        job = Job()
        job.generate_self(id, formatted_time)

        try:
            msg = json.dumps(job.to_dict()) # Converte o objeto Job para um dicionário e depois para JSON
            
            channel.basic_publish(exchange='', routing_key=KEY, body=msg) # Envia a mensagem para a fila
            print(f"[x] Mensagem enviada: {job.to_dict()}")
            
            with open('last_id.bin', 'wb') as f:
                id += 1
                f.write(bytes(str(id), 'utf-8'))

        except Exception as e:
            print(f"Erro ao enviar mensagem: {e}")
            break
    
    connection.close()

if __name__ == "__main__":
    main()
