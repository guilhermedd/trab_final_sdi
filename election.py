import socket
import configparser
import time
import threading
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Ler a configuração do arquivo
config = configparser.ConfigParser()
config.read('config.properties')

ip = config.get('TCP', 'IP')
consensus = int(config.get('TCP', 'PORT_CONSENSUS'))
ports = (
    int(config.get('TCP', 'PORT_1')),
    int(config.get('TCP', 'PORT_2')),
    int(config.get('TCP', 'PORT_3')),
)

def get_onlines(ip, ports):
    onlines = []
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            try:
                s.connect((ip, port))
                onlines.append(port)
            except (socket.timeout, ConnectionRefusedError):
                continue
    return onlines

def send_master_msg(ip, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ip, port))
            message = 'YOU_ARE_MASTER'
            s.sendall(message.encode())
            while True:
                response = s.recv(1024).decode('utf-8')
                if response == 'ACK':
                    logging.info(f'Received ACK from server. Port {port} is the new master')
                    time.sleep(5)  # Aguardando 5 segundos antes de enviar a próxima mensagem
                    s.sendall(message.encode())
                else:
                    break
    except ConnectionRefusedError:
        logging.error(f'Não foi possível conectar ao servidor em {ip}:{port}. O servidor pode estar offline.')
    except socket.timeout:
        logging.error(f'Tempo de conexão excedido ao tentar conectar a {ip}:{port}.')
    except Exception as e:
        logging.error(f'Ocorreu um erro: {e}')
    return False

def main():
    while True:
        onlines = get_onlines(ip, ports)
        for port in onlines:
            while True:
                if not send_master_msg(ip, port):
                    break
                time.sleep(5)

if __name__ == "__main__":
    main()
