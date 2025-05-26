import socket
import threading
import time
import queue # Usaremos a classe queue.Queue para uma fila thread-safe
import random
from .service import Service # Para criar instâncias de Service
from src.utils.logger_setup import get_logger # Importa o logger setup

class LoadBalancer:
    def __init__(self, host, port, max_queue_size, initial_num_services,
                 service_config, lb_name="LoadBalancer"):
        self.host = host
        self.port = port
        self.lb_name = f"{lb_name}_{port}"
        
        self.logger = get_logger(self.lb_name)

        self.request_queue = queue.Queue(maxsize=max_queue_size)
        self.services = [] 
        self.service_threads = [] 
        self.service_config = service_config
        self.next_service_port = port + 1 
        self.service_index_round_robin = 0

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10) 
        self.logger.info(f"Ouvindo em {self.host}:{self.port}")

        self._create_initial_services(initial_num_services)

        dispatcher_thread = threading.Thread(target=self._dispatch_messages, name=f"{self.lb_name}_DispatcherThread")
        dispatcher_thread.daemon = True
        dispatcher_thread.start()

    def _get_next_service_port(self):
        port_to_return = self.next_service_port
        self.next_service_port += 1
        return port_to_return

    def _create_service_instance(self):
        service_port = self._get_next_service_port()
        service_instance_name = f"Svc_{self.lb_name}_{service_port}"
        try:
            service = Service(
                host=self.service_config['host'],
                port=service_port,
                target_host=self.service_config['target_host'],
                target_port=self.service_config['target_port'],
                service_time_mean=self.service_config['time_mean'],
                service_time_std_dev=self.service_config['time_std_dev'],
                is_target_source=self.service_config['target_is_source'],
                service_name=service_instance_name
            )
            thread = threading.Thread(target=service.start, name=f"{service_instance_name}_Thread")
            thread.daemon = True
            thread.start()
            self.services.append(service)
            self.service_threads.append(thread)
            self.logger.info(f"Criado e iniciado serviço {service.service_name} em {service.host}:{service.port}")
            return service
        except Exception as e:
            self.logger.error(f"Falha ao criar serviço na porta {service_port}: {e}")
            return None

    def _create_initial_services(self, num_services):
        self.logger.info(f"Criando {num_services} serviços iniciais...")
        for _ in range(num_services):
            self._create_service_instance()
        self.logger.info(f"{len(self.services)} serviços criados.")

    def _register_time_lb(self, message_parts):
        current_time = time.time()
        last_timestamp_str = "N/A" 
        try:
            if len(message_parts) < 3:
                self.logger.debug(f"_register_time_lb: Mensagem muito curta: {message_parts}")
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
            self.logger.error(f"_register_time_lb: ERRO ao converter last_timestamp_str='{last_timestamp_str}'")
            last_timestamp = current_time 
            time_since_last = 0.0
        except IndexError:
             self.logger.error(f"_register_time_lb: ERRO de índice com message_parts: {message_parts}")
             last_timestamp = current_time
             time_since_last = 0.0
        
        self.logger.debug(f"_register_time_lb: msg_parts_in_len={len(message_parts)}, last_ts_str='{last_timestamp_str}', last_ts={last_timestamp:.6f}, curr_ts={current_time:.6f}, delta_calc_ms={time_since_last:.3f}")
        
        message_parts.append(f"{current_time:.6f}") 
        message_parts.append(f"{time_since_last:.3f}") 
        return message_parts

    def _is_service_free(self, service_instance):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5) 
                s.connect((service_instance.host, service_instance.port))
                s.sendall(b"ping")
                response = s.recv(1024).decode('utf-8')
                return response == "free"
        except (socket.timeout, ConnectionRefusedError, BrokenPipeError):
            self.logger.debug(f"Serviço {service_instance.service_name} não respondeu ao ping ou recusou conexão.")
            return False
        except Exception as e:
            self.logger.warning(f"Erro ao pingar serviço {service_instance.service_name}: {e}")
            return False

    def _dispatch_messages(self):
        """Pega mensagens da fila e envia para um serviço disponível, atualizando o tempo no LB."""
        while True:
            raw_enqueued_message = None # Para garantir que está definida em caso de reenfileiramento
            try:
                raw_enqueued_message = self.request_queue.get(timeout=1)
                message_parts = raw_enqueued_message.strip().split(';')
                
                # Este é o timestamp de quando a mensagem foi pega da fila e está prestes a ser processada pelo dispatcher
                ts_dispatch_processing_start = time.time() 

                if len(message_parts) >= 2:
                    ts_antes_fila_str = message_parts[-2] 
                    try:
                        ts_antes_fila = float(ts_antes_fila_str)
                        
                        tempo_no_lb_antes_de_enviar_ms = (ts_dispatch_processing_start - ts_antes_fila) * 1000.0
                        
                        message_parts[-1] = f"{tempo_no_lb_antes_de_enviar_ms:.3f}"
                        self.logger.debug(f"_dispatch_messages: Atualizado delta do LB para {tempo_no_lb_antes_de_enviar_ms:.3f} ms. Msg: {'_'.join(message_parts[:2])}")
                    except ValueError:
                        self.logger.error(f"_dispatch_messages: Erro ao converter ts_antes_fila_str='{ts_antes_fila_str}' para calcular tempo no LB. Usando placeholder original.")
                        # Se houver erro, o placeholder "0.000" (ou o que quer que esteja lá) permanece.
                
                message_to_send_to_service = ";".join(message_parts) # Mensagem com o delta do LB atualizado
                
                if not self.services:
                    self.logger.debug(f"Não há serviços ativos. Reenfileirando: {'_'.join(message_parts[:2])}")
                    self.request_queue.put(raw_enqueued_message) # Devolve a mensagem original, sem o delta do LB atualizado
                    time.sleep(1)
                    continue

                service_found = False
                # Lógica de Round Robin e envio
                initial_service_index = self.service_index_round_robin
                for _ in range(len(self.services) * 2): 
                    current_service = self.services[self.service_index_round_robin]
                    self.service_index_round_robin = (self.service_index_round_robin + 1) % len(self.services)

                    if self._is_service_free(current_service):
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket_to_service:
                                client_socket_to_service.connect((current_service.host, current_service.port))
                                # Envia a mensagem com o delta do LB atualizado
                                client_socket_to_service.sendall(message_to_send_to_service.encode('utf-8'))
                                ack = client_socket_to_service.recv(1024).decode('utf-8')
                                if ack == "ack_received":
                                    self.logger.debug(f"Mensagem '{message_to_send_to_service[:30].strip()}...' enviada para {current_service.service_name}")
                                    service_found = True
                                    break
                                else:
                                    self.logger.warning(f"{current_service.service_name} respondeu com '{ack}' em vez de ACK. Reenfileirando msg original.")
                                    self.request_queue.put(raw_enqueued_message) 
                                    service_found = True 
                                    break 
                        except ConnectionRefusedError:
                            self.logger.debug(f"{current_service.service_name} recusou conexão. Tentando próximo.")
                            pass 
                        except Exception as e:
                            self.logger.error(f"Erro ao enviar para {current_service.service_name}: {e}. Reenfileirando msg original.")
                            self.request_queue.put(raw_enqueued_message) 
                            service_found = True 
                            break
                
                if not service_found:
                    self.logger.debug(f"Nenhum serviço livre. Reenfileirando msg original: {'_'.join(message_parts[:2])}")
                    self.request_queue.put(raw_enqueued_message) 
                    time.sleep(0.1)

                self.request_queue.task_done()
            except queue.Empty:
                pass # Fila vazia, continua esperando
            except Exception as e:
                self.logger.critical(f"Erro crítico no dispatcher: {e}. Mensagem original: {raw_enqueued_message}")
                if raw_enqueued_message: # Tenta reenfileirar se uma mensagem foi pega
                    try:
                        self.request_queue.put(raw_enqueued_message)
                        self.logger.info(f"Mensagem {raw_enqueued_message[:30]} reenfileirada após erro crítico.")
                    except Exception as e_requeue:
                        self.logger.error(f"Falha ao reenfileirar mensagem após erro crítico: {e_requeue}")


    def _change_service_targets(self, num_new_services, client_socket_from_source):
        self.logger.info(f"Recebida configuração para alterar para {num_new_services} serviços.")

        # TODO: Parar threads dos serviços antigos de forma mais graciosa (usar um event, por exemplo)
        for service_thread in self.service_threads:
            # Aqui precisaríamos de um método stop() no Service ou um event para sinalizar
            # Como são daemon, elas eventualmente pararão, mas não é imediato nem limpo.
            pass
        self.services.clear()
        self.service_threads.clear() 
        self.logger.info("Serviços antigos removidos (lista limpa).")

        for _ in range(num_new_services):
            self._create_service_instance()

        self.logger.info(f"{len(self.services)} novos serviços criados.")
        try:
            client_socket_from_source.sendall("Configuration has finished\n".encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Erro ao enviar confirmação de config: {e}")

    def handle_incoming_connection(self, client_socket, address):
        self.logger.info(f"Conexão aceita de {address}")
        try:
            data = client_socket.recv(1024)
            if not data:
                self.logger.debug(f"Nenhum dado recebido de {address}. Fechando.")
                return

            message = data.decode('utf-8').strip()
            self.logger.debug(f"Mensagem recebida de {address}: {message[:100]}")

            if message.lower() == "ping":
                if self.request_queue.full():
                    client_socket.sendall(b"busy")
                    self.logger.debug(f"Ping de {address} respondido com: busy (fila cheia)")
                else:
                    client_socket.sendall(b"free")
                    self.logger.debug(f"Ping de {address} respondido com: free")
            elif message.startswith("config;"):
                parts = message.split(';')
                try:
                    num_services = int(parts[1])
                    self._change_service_targets(num_services, client_socket)
                except (IndexError, ValueError) as e:
                    self.logger.error(f"Formato de mensagem de config inválido: {message}. Erro: {e}")
                    client_socket.sendall("Config error\n".encode('utf-8'))
            else:
                if self.request_queue.full():
                    self.logger.warning(f"Fila cheia. Mensagem '{message[:30]}...' de {address} DESCARTADA.")
                    client_socket.sendall("busy_queue_full".encode('utf-8'))
                else:
                    message_parts = message.split(';')
                    message_parts = self._register_time_lb(message_parts)
                    message_parts.append(f"{time.time():.6f}") # ts_antes_fila_lb
                    message_parts.append("0.000") # Placeholder para delta_tempo_no_LB
                    
                    enqueued_message = ";".join(message_parts) + "\n"
                    self.request_queue.put(enqueued_message)
                    self.logger.debug(f"Mensagem '{enqueued_message[:30].strip()}...' enfileirada de {address}.")
                    client_socket.sendall("ack_queued".encode('utf-8'))

        except socket.timeout:
            self.logger.warning(f"Timeout na conexão com {address}.")
        except Exception as e:
            self.logger.error(f"Erro ao lidar com conexão de {address}: {e}")
        finally:
            client_socket.close()
            self.logger.debug(f"Conexão com {address} fechada.")

    def start(self):
        threading.current_thread().name = f"{self.lb_name}_AcceptThread"
        while True: # TODO: Adicionar um mecanismo de parada graciosa para o LB
            try:
                client_socket, address = self.server_socket.accept()
                conn_thread_name = f"{self.lb_name}_Conn_{address[1]}"
                conn_thread = threading.Thread(target=self.handle_incoming_connection, args=(client_socket, address), name=conn_thread_name)
                conn_thread.daemon = True
                conn_thread.start()
            except Exception as e:
                self.logger.critical(f"Erro crítico ao aceitar nova conexão: {e}. O LB pode parar de funcionar.")
                time.sleep(1) # Evita tight loop em caso de erro persistente no accept
        
        # self.logger.info("Encerrando LoadBalancer.") # Só seria alcançado com um break no loop


if __name__ == '__main__':
    main_logger_lb_test = get_logger("LoadBalancerTestMain")
    main_logger_lb_test.info("Iniciando teste individual do LoadBalancer...")

    service_config_lb1_test = {
        'host': 'localhost',
        'target_host': 'localhost', 
        'target_port': 6000, 
        'time_mean': 0.05,
        'time_std_dev': 0.01,
        'target_is_source': False
    }

    lb_test_node = LoadBalancer(
        host='localhost',
        port=5000,
        max_queue_size=10,
        initial_num_services=1,
        service_config=service_config_lb1_test,
        lb_name="TestLB"
    )
    lb_test_node.start()
    main_logger_lb_test.info("Teste individual do LoadBalancer encerrado (se start() não fosse bloqueante).")
