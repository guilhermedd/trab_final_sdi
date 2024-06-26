import socket
import configparser
import time

config = configparser.ConfigParser()
config.read('config.properties')

ip = config.get('TCP', 'IP')
ports = (
    int(config.get('TCP', 'PORT_1')),
    int(config.get('TCP', 'PORT_2')),
    int(config.get('TCP', 'PORT_3')),
)

def communicate(master_port, minions_ports):
    try:
        # Conectar ao mestre
        tcp_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket1.connect((ip, master_port))
        
        while True:
            msg_master = bytes("MASTER", 'utf-8')
            tcp_socket1.sendall(msg_master)
            
            for minion_port in minions_ports:
                try:
                    # Conectar aos minions
                    tcp_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_socket2.connect((ip, minion_port))
                    msg_minion = bytes("MINION", 'utf-8')
                    tcp_socket2.sendall(msg_minion)
                except socket.error as e:
                    print(f'Error communicating with minion at port {minion_port}: {e}')
                finally:
                    tcp_socket2.close()
            
            time.sleep(1)
            
    except socket.error as e:
        print(f'Error in communication: {e}')
    finally:
        tcp_socket1.close()

# Loop principal
while True:
    for i in range(len(ports)):
        tmp = list(ports)  # Cópia da lista de portas
        tmp.remove(ports[i])  # Remove o elemento atual da lista
        
        try:
            communicate(ports[i], tmp)
        except socket.error as e:
            print(f'Error communicating with port {ports[i]}: {e}')
            break
        time.sleep(1)


############################


import socket
import threading

# Configurações do servidor TCP
server_ip = '0.0.0.0'  # Ou 'localhost' se estiver rodando localmente
server_port = 5000  # Porta do servidor que vai receber as mensagens

def handle_client(client_socket):
    try:
        while True:
            # Recebe a mensagem do cliente
            data = client_socket.recv(1024)
            if not data:
                break
            
            # Decodifica a mensagem recebida (assumindo que é UTF-8)
            message = data.decode('utf-8').strip()
            
            # Lógica para lidar com diferentes tipos de mensagem
            if message == 'MASTER':
                print(f'Received MASTER message from {client_socket.getpeername()}')
                # Lógica para processar a mensagem do mestre
            elif message == 'MINION':
                print(f'Received MINION message from {client_socket.getpeername()}')
                # Lógica para processar a mensagem do minion
            
            # Exemplo de resposta ao cliente
            response = 'Message received'
            client_socket.sendall(response.encode('utf-8'))
            
    except Exception as e:
        print(f'Error handling client: {e}')
    finally:
        client_socket.close()

# Função principal do servidor
def start_server():
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((server_ip, server_port))
        server_socket.listen(5)
        print(f'Server listening on {server_ip}:{server_port}')
        
        while True:
            client_socket, client_address = server_socket.accept()
            print(f'Accepted connection from {client_address}')
            
            # Cria uma thread para lidar com o cliente
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()
            
    except Exception as e:
        print(f'Server error: {e}')
    finally:
        server_socket.close()

# Inicia o servidor
if __name__ == '__main__':
    start_server()
