import socket
import threading
import time
import random
from src.utils.logger_setup import get_logger

class Service:
    def __init__(self, host, port, target_host, target_port, service_time_mean, service_time_std_dev, is_target_source=False, service_name="Service"):
        self.host = host
        self.port = port
        self.target_host = target_host
        self.target_port = target_port
        self.service_time_mean = service_time_mean
        self.service_time_std_dev = service_time_std_dev
        self.is_target_source = is_target_source
        self.service_name = service_name if "_Managed_By_" in service_name else f"{service_name}_{port}"
        
        # Configuração do Logger
        self.logger = get_logger(self.service_name)

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.current_message_to_process = None
        self.processing_lock = threading.Lock()
        self.logger.info(f"Ouvindo em {self.host}:{self.port}")

    def _register_time(self, message_parts):
        current_time = time.time()
        last_timestamp_str = "N/A" # Default para logs
        try:
            if len(message_parts) < 3:
                self.logger.debug(f"_register_time: Mensagem muito curta: {message_parts}")
                last_timestamp = current_time
                time_since_last = 0.0
            elif len(message_parts) == 3: 
                last_timestamp_str = message_parts[2]
                last_timestamp = float(last_timestamp_str)
                time_since_last = (current_time - last_timestamp) * 1000
            else: 
                last_timestamp_str = message_parts[-2]
                last_timestamp = float(last_timestamp_str)
                time_since_last = (current_time - last_timestamp) * 1000
        except ValueError:
            self.logger.error(f"_register_time: ERRO ao converter last_timestamp_str='{last_timestamp_str}'")
            last_timestamp = current_time 
            time_since_last = 0.0
        except IndexError:
             self.logger.error(f"_register_time: ERRO de índice com message_parts: {message_parts}")
             last_timestamp = current_time
             time_since_last = 0.0
        
        self.logger.debug(f"_register_time: msg_parts_in_len={len(message_parts)}, last_ts_str='{last_timestamp_str}', last_ts={last_timestamp:.6f}, curr_ts={current_time:.6f}, delta_calc_ms={time_since_last:.3f}")
        
        message_parts.append(f"{current_time:.6f}") 
        message_parts.append(f"{time_since_last:.3f}") 
        return message_parts

    def _simulate_processing(self, message_parts):
        processing_time_sec = abs(random.gauss(self.service_time_mean, self.service_time_std_dev))
        self.logger.debug(f"Simulando processamento por {processing_time_sec:.4f} segundos.")
        time.sleep(processing_time_sec)

        timestamp_saida_processamento = time.time()
        timestamp_chegada_processamento = 0.0 # Default
        if len(message_parts) >=2 : # Deve ter pelo menos current_time e time_since_last adicionados por _register_time
             try:
                timestamp_chegada_processamento = float(message_parts[-2]) # Este é o current_time adicionado por _register_time
             except ValueError:
                self.logger.error(f"_simulate_processing: Erro ao converter timestamp_chegada_processamento de '{message_parts[-2]}'")
                timestamp_chegada_processamento = timestamp_saida_processamento # Evita erro, mas o delta será 0
        
        tempo_de_processamento_ms = (timestamp_saida_processamento - timestamp_chegada_processamento) * 1000
        self.logger.debug(f"Tempo de processamento real: {tempo_de_processamento_ms:.3f} ms")

        message_parts.append(f"{timestamp_saida_processamento:.6f}")
        message_parts.append(f"{tempo_de_processamento_ms:.3f}")
        return message_parts

    def _send_to_target(self, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as target_socket:
                target_socket.connect((self.target_host, self.target_port))
                target_socket.sendall(message.encode('utf-8'))
                self.logger.debug(f"Mensagem '{message[:30].strip()}...' enviada para {self.target_host}:{self.target_port}")
        except Exception as e:
            self.logger.error(f"Erro ao enviar para {self.target_host}:{self.target_port}: {e}")

    def handle_client_connection(self, client_socket, address):
        self.logger.info(f"Conexão aceita de {address}")
        try:
            data = client_socket.recv(1024)
            if not data:
                self.logger.debug(f"Nenhum dado recebido de {address}. Fechando conexão.")
                return

            message = data.decode('utf-8').strip()
            self.logger.debug(f"Mensagem recebida: {message[:100]}") # Loga início de mensagens longas

            if message.lower() == "ping":
                with self.processing_lock:
                    status = "busy" if self.current_message_to_process else "free"
                client_socket.sendall(status.encode('utf-8'))
                self.logger.debug(f"Respondeu ao ping de {address} com: {status}")
            else:
                with self.processing_lock:
                    if self.current_message_to_process:
                        self.logger.warning(f"Ocupado. Mensagem '{message[:30]}...' de {address} DESCARTADA.")
                        client_socket.sendall("busy".encode('utf-8'))
                        return
                    self.current_message_to_process = message 

                client_socket.sendall("ack_received".encode('utf-8')) 
                self.logger.debug(f"ACK enviado para {address} por msg '{message[:30]}...'")

                message_parts = message.split(';')
                message_parts = self._register_time(message_parts)
                message_parts = self._simulate_processing(message_parts)

                processed_message = ";".join(message_parts)
                self.logger.info(f"Mensagem '{processed_message[:30]}...' processada. Enviando...")
                self.logger.debug(f"Conteúdo msg processada: {processed_message}")
                self._send_to_target(processed_message + "\n")

                with self.processing_lock:
                    self.current_message_to_process = None
                    self.logger.debug("Serviço liberado.")

        except socket.timeout:
            self.logger.warning(f"Timeout na conexão com {address}.")
        except Exception as e:
            self.logger.error(f"Erro ao lidar com cliente {address}: {e}")
        finally:
            client_socket.close()
            self.logger.debug(f"Conexão com {address} fechada.")

    def start(self):
        threading.current_thread().name = f"{self.service_name}_AcceptThread"
        while True:
            try:
                client_socket, address = self.server_socket.accept()
                conn_thread_name = f"{self.service_name}_Conn_{address[1]}"
                client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket, address), name=conn_thread_name)
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                self.logger.error(f"Erro crítico ao aceitar conexão: {e}. Encerrando serviço.")
                break # Sai do loop se o socket principal falhar
        
        if self.server_socket:
            self.server_socket.close()
            self.logger.info("Socket do servidor fechado.")


if __name__ == '__main__':
    main_logger_svc_test = get_logger("ServiceTestMain")
    main_logger_svc_test.info("Iniciando teste individual do Service...")
    
    service_node = Service(
        host='localhost',
        port=7001, # Porta exemplo diferente para teste individual
        target_host='localhost', 
        target_port=4000, 
        service_time_mean=0.1, 
        service_time_std_dev=0.02,
        is_target_source=True, 
        service_name="TestService" # O construtor adicionará a porta a este nome base
    )
    service_node.start() # Bloqueante
    main_logger_svc_test.info("Teste individual do Service encerrado (se start() não fosse bloqueante).")