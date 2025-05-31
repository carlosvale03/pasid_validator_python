import socket
import threading
import time
import queue 
import random
# Importa Service e SentimentClassifier. Se SentimentClassifier não estiver em service.py,
# e for necessário aqui (não é mais, pois o Service lida com a instância), ajuste.
from .service import Service, SentimentClassifier # Adicionado SentimentClassifier para o carregamento no LB
from src.utils.logger_setup import get_logger 
import os
import torch # Adicionado para torch.device

from transformers import BertTokenizer

class LoadBalancer:
    def __init__(self, host, port, max_queue_size, initial_num_services,
                 service_config, lb_name="LoadBalancer"): # service_config agora inclui 'has_ia'
        self.host = host
        self.port = port
        self.lb_name = f"{lb_name}_{port}"
        self.logger = get_logger(self.lb_name)

        self.request_queue = queue.Queue(maxsize=max_queue_size)
        self.services = [] 
        self.active_service_objects_lock = threading.Lock() # Lock para proteger self.services
        self.service_config_template = service_config # Guarda o template da config do serviço
        self.next_service_port = port + 1 
        self.service_index_round_robin = 0

        # --- Carregamento Condicional do Modelo de IA Compartilhado ---
        self.has_ia = self.service_config_template.get('has_ia', False)
        self.shared_ai_model = None
        self.shared_tokenizer = None
        
        if self.has_ia:
            self.logger.info(f"LB {self.lb_name}: Configurado para ter IA. Tentando carregar modelo compartilhado...")
            try:
                # model_path e bert_model_name são necessários se has_ia for True
                model_path = self.service_config_template.get('model_path')
                bert_model_name = self.service_config_template.get('bert_model_name')

                if not model_path or not bert_model_name:
                    raise ValueError("Para 'has_ia: True', 'model_path' e 'bert_model_name' devem ser fornecidos na service_config.")

                self.shared_model_max_len = self.service_config_template.get('max_len', 180)
                self.shared_model_num_classes = self.service_config_template.get('num_classes', 3)
                self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
                self.logger.info(f"LB {self.lb_name}: Usando dispositivo: {self.device} para modelo compartilhado.")

                self.logger.info(f"LB {self.lb_name}: Carregando Tokenizer compartilhado para: '{bert_model_name}' (via cache HF)")
                self.shared_tokenizer = BertTokenizer.from_pretrained(bert_model_name)
                
                self.logger.info(f"LB {self.lb_name}: Instanciando SentimentClassifier compartilhado com base: '{bert_model_name}' (via cache HF)")
                self.shared_ai_model = SentimentClassifier(self.shared_model_num_classes, pre_trained_model_path_or_name=bert_model_name)
                
                state_dict_file_name = 'posicionamento_BERTimabauPT-BR_211124.bin'
                path_opt1 = os.path.join(model_path, 'models', state_dict_file_name)
                path_opt2 = os.path.join(model_path, state_dict_file_name)
                actual_state_dict_path = None
                if os.path.exists(path_opt1): actual_state_dict_path = path_opt1
                elif os.path.exists(path_opt2): actual_state_dict_path = path_opt2
                else: raise FileNotFoundError(f"Arquivo state_dict (.bin) '{state_dict_file_name}' NÃO encontrado para LB em '{path_opt1}' ou '{path_opt2}'")

                self.logger.info(f"LB {self.lb_name}: Carregando state_dict compartilhado fine-tuned de: {actual_state_dict_path}")
                self.shared_ai_model.load_state_dict(torch.load(actual_state_dict_path, map_location=self.device))
                self.shared_ai_model = self.shared_ai_model.to(self.device)
                self.shared_ai_model.eval()
                self.logger.info(f"LB {self.lb_name}: Modelo de IA e Tokenizer compartilhados carregados e prontos.")
            except Exception as e:
                self.logger.critical(f"LB {self.lb_name}: Erro CRÍTICO ao carregar modelo/tokenizer compartilhado: {e}", exc_info=True)
                self.has_ia = False # Desabilita IA se o carregamento falhar para este LB
                self.shared_ai_model = None
                self.shared_tokenizer = None
        else:
            self.logger.info(f"LB {self.lb_name}: Configurado para NÃO ter IA. Serviços atuarão como encaminhadores.")
        # --- FIM DO CARREGAMENTO COMPARTILHADO ---

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        
        model_to_pass = self.shared_ai_model if self.has_ia else None
        tokenizer_to_pass = self.shared_tokenizer if self.has_ia else None

        # Se este LB deveria ter IA, mas o modelo compartilhado falhou ao carregar,
        # os serviços filhos não receberão o modelo e devem ser informados disso.
        service_expects_ia = self.has_ia # O serviço espera IA se o LB que o cria espera IA

        if self.has_ia and (not model_to_pass or not tokenizer_to_pass):
            self.logger.error(f"LB {self.lb_name} está configurado para ter IA, mas o modelo/tokenizer compartilhado não carregou. Serviço {service_instance_name} não terá IA funcional.")
            # model_to_pass e tokenizer_to_pass já são None, service_expects_ia ainda é True

        try:
            service = Service(
                host=self.service_config_template.get('host', 'localhost'),
                port=service_port,
                target_host=self.service_config_template['target_host'],
                target_port=self.service_config_template['target_port'],
                ai_model_instance=model_to_pass,
                tokenizer_instance=tokenizer_to_pass,
                is_target_source=self.service_config_template['target_is_source'],
                service_name=service_instance_name,
                max_len=self.service_config_template.get('max_len', 180),
                expected_to_have_ia=service_expects_ia, # <<< PASSANDO A EXPECTATIVA
                service_time_mean=self.service_config_template.get('time_mean', 10),
                service_time_std_dev=self.service_config_template.get('time_std_dev', 2)
            )
            
            # ... (resto do _create_service_instance como na última versão) ...
            thread = threading.Thread(target=service.start, name=f"{service_instance_name}_Thread")
            thread.daemon = True
            thread.start()
            with self.active_service_objects_lock:
                self.services.append(service)
            self.logger.info(f"Thread para serviço {service.service_name} (espera IA: {service_expects_ia}, provida por LB: {self.has_ia and bool(model_to_pass)}) iniciada.")
            return service
            
        except KeyError as e:
            self.logger.error(f"Falha ao criar {service_instance_name}: Chave de config ausente no template - {e}.")
            return None
        except Exception as e:
            self.logger.error(f"Falha ao criar {service_instance_name}: {e}", exc_info=True)
            return None
            
    def _create_initial_services(self, num_services):
        self.logger.info(f"Criando {num_services} serviços iniciais...")
        successfully_created_count = 0
        for _ in range(num_services):
            if self._create_service_instance(): # Já loga sucesso/falha da criação
                successfully_created_count += 1
        self.logger.info(f"{successfully_created_count}/{num_services} serviços iniciais criados/iniciados.")

    def _register_time_lb(self, message_parts):
        # ... (mesma lógica robusta de _register_time_lb da sua última versão) ...
        current_time = time.time()
        last_timestamp_str = "N/A"; last_timestamp = current_time; time_since_last = 0.0
        try:
            if not message_parts: self.logger.warning("_register_time_lb: message_parts vazio."); return [f"{current_time:.6f}", "0.000"]
            relevant_timestamp_index = -1
            if len(message_parts) == 4 and all(c.isdigit() or c == '.' for c in message_parts[2]): relevant_timestamp_index = 2
            elif len(message_parts) > 2 and all(c.isdigit() or c == '.' for c in message_parts[-2]): relevant_timestamp_index = -2
            else: self.logger.debug(f"_register_time_lb: Não foi possível determinar último TS. Parts: {message_parts}")
            if relevant_timestamp_index != -1 and -len(message_parts) <= relevant_timestamp_index < len(message_parts) :
                 last_timestamp_str = message_parts[relevant_timestamp_index]; last_timestamp = float(last_timestamp_str)
                 time_since_last = (current_time - last_timestamp) * 1000
        except ValueError: self.logger.error(f"_register_time_lb: ERRO ao converter '{last_timestamp_str}'. Parts: {message_parts}")
        except IndexError: self.logger.error(f"_register_time_lb: ERRO índice. Parts: {message_parts}")
        message_parts.append(f"{current_time:.6f}"); message_parts.append(f"{time_since_last:.3f}")
        return message_parts

    def _is_service_free(self, service_instance):
        # ... (mesma lógica de _is_service_free da sua última versão, 
        #      verificando se service_instance é válido e se tem ai_model se este LB tiver IA) ...
        if not service_instance: # Checagem básica
            return False
        # Se este LB tem IA, seus serviços DEVEM ter um modelo. Se não, pingar pode não fazer sentido se ele não puder processar.
        # No entanto, um serviço sem IA (do LB1) ainda deve responder ao ping como 'free' se não estiver ocupado encaminhando.
        # A lógica atual do Service.handle_incoming_connection para ping não depende do ai_model.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2) 
                s.connect((service_instance.host, service_instance.port))
                s.sendall(b"ping")
                response_bytes = s.recv(1024)
                if not response_bytes: self.logger.debug(f"Serviço {service_instance.service_name} fechou conexão no ping."); return False
                response = response_bytes.decode('utf-8')
                return response == "free"
        except (socket.timeout, ConnectionRefusedError, BrokenPipeError):
            self.logger.debug(f"Serviço {service_instance.service_name} não respondeu/recusou ping.")
            return False
        except Exception as e:
            self.logger.warning(f"Erro ao pingar serviço {service_instance.service_name}: {e}")
            return False

    def _dispatch_messages(self):
        # ... (mesma lógica de _dispatch_messages da sua última versão, 
        #      mas a lista active_services agora é filtrada para serviços válidos) ...
        while True:
            raw_enqueued_message = None
            try:
                raw_enqueued_message = self.request_queue.get(timeout=0.5)
                message_parts = raw_enqueued_message.strip().split(';')
                ts_dispatch_attempt = time.time() 
                if len(message_parts) >= 2: 
                    ts_antes_fila_str = message_parts[-2] 
                    try:
                        ts_antes_fila = float(ts_antes_fila_str)
                        tempo_real_na_fila_ms = (ts_dispatch_attempt - ts_antes_fila) * 1000.0
                        message_parts[-1] = f"{tempo_real_na_fila_ms:.3f}"
                        self.logger.debug(f"Dispatcher: Atualizado delta fila LB para {tempo_real_na_fila_ms:.3f} ms. Msg: {'_'.join(message_parts[:2])}")
                    except ValueError: self.logger.error(f"Dispatcher: Erro ao converter ts_antes_fila='{ts_antes_fila_str}'")
                message_to_send_to_service = ";".join(message_parts)
                with self.active_service_objects_lock:
                    # Filtra serviços que podem ter falhado na inicialização do modelo (se este LB tiver IA)
                    # ou simplesmente serviços que existem.
                    active_services = [s for s in self.services if s and (not self.has_ia or (hasattr(s, 'ai_model') and s.ai_model))]
                if not active_services:
                    self.logger.warning(f"Dispatcher: Nenhum serviço ATIVO/VÁLIDO. Reenfileirando: {'_'.join(message_parts[:2])}")
                    self.request_queue.put(raw_enqueued_message); time.sleep(0.5); continue
                service_found_and_sent = False
                num_active_services = len(active_services)
                for i in range(num_active_services): 
                    current_service_idx_in_active_list = (self.service_index_round_robin + i) % num_active_services
                    current_service = active_services[current_service_idx_in_active_list]
                    if self._is_service_free(current_service):
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket_to_service:
                                client_socket_to_service.connect((current_service.host, current_service.port))
                                client_socket_to_service.sendall(message_to_send_to_service.encode('utf-8'))
                                client_socket_to_service.settimeout(2.0)
                                ack = client_socket_to_service.recv(1024).decode('utf-8')
                                if ack.strip() == "ack_received":
                                    self.logger.debug(f"Msg '{message_parts[0]}_{message_parts[1]}' enviada para {current_service.service_name} e ACK_RECEIVED.")
                                    self.service_index_round_robin = (current_service_idx_in_active_list + 1) % num_active_services
                                    service_found_and_sent = True; break
                                else: self.logger.warning(f"{current_service.service_name} respondeu '{ack}'.")
                        except Exception as e: self.logger.error(f"Erro ao enviar para {current_service.service_name}: {e}.")
                if not service_found_and_sent:
                    self.logger.debug(f"Nenhum serviço livre. Reenfileirando: {'_'.join(message_parts[:2])}")
                    self.request_queue.put(raw_enqueued_message); time.sleep(0.1)
                self.request_queue.task_done()
            except queue.Empty: pass 
            except Exception as e:
                self.logger.critical(f"Erro no dispatcher: {e}. Msg: {raw_enqueued_message}", exc_info=True)
                if raw_enqueued_message: 
                    try: self.request_queue.put(raw_enqueued_message)
                    except Exception as er: self.logger.error(f"Falha ao reenfileirar: {er}")

    def _change_service_targets(self, num_new_services, client_socket_from_source):
        self.logger.info(f"Configurando para {num_new_services} serviços.")
        with self.active_service_objects_lock:
            self.services.clear() 
        self.logger.info("Listas de serviços antigos limpas.")
        successfully_created_services_count = 0
        for i in range(num_new_services):
            self.logger.info(f"Tentando criar e iniciar serviço {i+1}/{num_new_services}...")
            if self._create_service_instance():
                successfully_created_services_count +=1
        
        self.logger.info(f"{successfully_created_services_count}/{num_new_services} novos serviços foram INICIADOS (threads iniciadas).")
        
        # Responde ao Source que a reconfiguração foi iniciada.
        # A espera pelo carregamento completo do modelo (se este LB tiver IA) é responsabilidade do Source.
        confirmation_message = "Configuration has finished\n"
        if successfully_created_services_count != num_new_services:
            confirmation_message = f"Configuration Alert: Only {successfully_created_services_count}/{num_new_services} services initiated successfully.\n"
            self.logger.warning(confirmation_message.strip())
        try:
            client_socket_from_source.sendall(confirmation_message.encode('utf-8'))
            self.logger.info("Confirmação de início de reconfig enviada ao Source.")
        except Exception as e:
            self.logger.error(f"Erro ao enviar confirmação de config ao Source: {e}")

    def handle_incoming_connection(self, client_socket, address):
        # ... (como na sua última versão, que já envia a resposta de config dentro de _change_service_targets) ...
        self.logger.info(f"Conexão aceita de {address}")
        try:
            data = client_socket.recv(4096) 
            if not data: self.logger.debug(f"Nenhum dado de {address}."); return
            message = data.decode('utf-8').strip()
            self.logger.debug(f"Mensagem recebida de {address}: {message[:100].replace(chr(0), '')}")
            if message.lower() == "ping":
                status_msg = b"busy" if self.request_queue.full() else b"free"
                client_socket.sendall(status_msg); self.logger.debug(f"Ping de {address} respondido com: {status_msg.decode()}")
            elif message.startswith("config;"):
                parts = message.split(';');
                try:
                    num_services = int(parts[1])
                    self._change_service_targets(num_services, client_socket) 
                except (IndexError, ValueError) as e:
                    self.logger.error(f"Formato de msg de config inválido: {message}. Erro: {e}")
                    try: client_socket.sendall("Config error\n".encode('utf-8'))
                    except Exception: pass 
            else:
                if self.request_queue.full():
                    self.logger.warning(f"Fila cheia. Msg '{message[:30]}...' de {address} DESCARTADA.")
                    try: client_socket.sendall("busy_queue_full".encode('utf-8'))
                    except Exception: pass
                else:
                    message_parts = message.split(';')
                    message_parts = self._register_time_lb(message_parts) 
                    message_parts.append(f"{time.time():.6f}")  
                    message_parts.append("0.000")               
                    enqueued_message = ";".join(message_parts)
                    self.request_queue.put(enqueued_message)
                    self.logger.debug(f"Mensagem '{message_parts[0]}_{message_parts[1]}' enfileirada de {address}.")
                    try: client_socket.sendall("ack_queued".encode('utf-8'))
                    except Exception: pass
        except socket.timeout: self.logger.warning(f"Timeout na conexão com {address}.")
        except Exception as e: self.logger.error(f"Erro ao lidar com conexão de {address}: {e}", exc_info=True)
        finally:
            if client_socket: 
                try: client_socket.close()
                except Exception: pass

    def start(self):
        # ... (como na sua última versão) ...
        threading.current_thread().name = f"{self.lb_name}_AcceptThread"
        self.logger.info(f"{self.lb_name} iniciado e pronto para aceitar conexões (IA Habilitada: {self.has_ia}).")
        while True: 
            try:
                client_socket, address = self.server_socket.accept()
                conn_thread_name = f"{self.lb_name}_Conn_{address[1]}"
                conn_thread = threading.Thread(target=self.handle_incoming_connection, args=(client_socket, address), name=conn_thread_name)
                conn_thread.daemon = True; conn_thread.start()
            except OSError: self.logger.info(f"Socket de escuta fechado para {self.lb_name}. Encerrando."); break
            except Exception as e: self.logger.critical(f"Erro ao aceitar conexão: {e}. Tentando continuar.", exc_info=True); time.sleep(1)
        if hasattr(self, 'server_socket') and self.server_socket:
            try: self.server_socket.close(); self.logger.info(f"Socket do servidor de {self.lb_name} fechado.")
            except Exception as e: self.logger.error(f"Erro ao fechar socket do {self.lb_name}: {e}")

if __name__ == '__main__':
    main_logger_lb_test = get_logger("LoadBalancerTestMain") 
    main_logger_lb_test.info("Iniciando teste individual do LoadBalancer...")
    CAMINHO_MODELO_BERT_TESTE_HOST = "D:/IC/youtube_model_1124" 
    if not os.path.exists(CAMINHO_MODELO_BERT_TESTE_HOST):
        main_logger_lb_test.error(f"Caminho do modelo para teste não encontrado: {CAMINHO_MODELO_BERT_TESTE_HOST}")
    else:
        # Testando LB COM IA
        service_config_lb_test_with_ia = {
            'host': 'localhost', 'target_host': 'localhost', 'target_port': 4000, 
            'time_mean': 0, 'time_std_dev': 0, 'target_is_source': True,
            'model_path': CAMINHO_MODELO_BERT_TESTE_HOST, 
            'bert_model_name':'neuralmind/bert-large-portuguese-cased',
            'num_classes': 3, 'max_len': 180,
            'has_ia': True # Importante para o teste
        }
        lb_test_node = LoadBalancer(host='localhost', port=5000, max_queue_size=10,
                                    initial_num_services=1, service_config=service_config_lb_test_with_ia,
                                    lb_name="TestLB_With_IA")
        try: lb_test_node.start()
        except KeyboardInterrupt: main_logger_lb_test.info("Teste do LoadBalancer interrompido.")
        finally:
            if hasattr(lb_test_node, 'server_socket') and lb_test_node.server_socket:
                lb_test_node.server_socket.close()
            main_logger_lb_test.info("Teste individual do LoadBalancer encerrado.")