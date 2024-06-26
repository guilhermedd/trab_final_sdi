import socket
import threading
import logging
import time
import configparser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

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

def send_master_msg(ip, port, timeout=2):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((ip, port))
            message = 'YOU_ARE_MASTER'
            s.sendall(message.encode())
            
            while True:
                response = s.recv(1024).decode('utf-8')
                if response == 'ACK':
                    logging.info(f'Received ACK from server. Port {port} is the new master')
                    time.sleep(3)  # Aguardando 5 segundos antes de enviar a próxima mensagem
                    s.sendall(message.encode())
                else:
                    break
    except socket.timeout:
        logging.error(f'Tempo de conexão excedido ao tentar conectar a {ip}:{port}.')
    except ConnectionRefusedError:
        logging.error(f'Não foi possível conectar ao servidor em {ip}:{port}. O servidor pode estar offline.')
    except Exception as e:
        logging.error(f'Ocorreu um erro durante a comunicação: {e}')

def get_values(ip, ports, timeout=2):
    vals = []
    for port in ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = 'YOUR_ID'
                s.sendall(message.encode())
                
                while True:
                    response = s.recv(1024).decode('utf-8')
                    if response.startswith('ID'):
                        id = response.split(':')[1]
                        logging.info(f'Received ACK from server. Port {port} is the new master')
                        time.sleep(1)  # Aguardando 1 segundo antes de enviar a próxima mensagem
                        s.sendall("ACK".encode())
                        vals.append([port, int(id)])
                    else:
                        break
        except socket.timeout:
            logging.error(f'Tempo de conexão excedido ao tentar conectar a {ip}:{port}.')
        except ConnectionRefusedError:
            logging.error(f'Não foi possível conectar ao servidor em {ip}:{port}. O servidor pode estar offline.')
        except Exception as e:
            logging.error(f'Ocorreu um erro durante a comunicação: {e}')
    
    if vals:
        vals.sort(key=lambda x: x[1])
        return vals[0]
    return None, None

def main():
    while True:
        onlines = get_onlines(ip, ports)
        if onlines:
            port, id = get_values(ip, onlines)
            if port is not None:
                aux = onlines[:]
                aux.remove(port)
                send_master_msg(ip, port)
            time.sleep(2)
        else:
            logging.warning('Nenhum servidor online encontrado. Tentando novamente em breve.')
            time.sleep(5)

if __name__ == "__main__":
    main()
