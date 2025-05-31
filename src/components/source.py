import socket
import threading
import time
import statistics
import random 
from src.utils.logger_setup import get_logger

class Source:
    def __init__(self, listen_host, listen_port, target_lb1_host, target_lb1_port,
                 arrival_delay_ms, max_messages_per_cycle,
                 qtd_services_variation, config_target_lb_host, config_target_lb_port,
                 source_name="Source"):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_lb1_host = target_lb1_host
        self.target_lb1_port = target_lb1_port
        self.arrival_delay_sec = arrival_delay_ms / 1000.0
        self.max_messages_per_cycle = max_messages_per_cycle
        self.qtd_services_variation = qtd_services_variation
        self.config_target_lb_host = config_target_lb_host
        self.config_target_lb_port = config_target_lb_port
        self.source_name = source_name
        self.logger = get_logger(f"{self.source_name}_{self.listen_port}")

        self.server_socket = None # Será inicializado em start()
        self.listener_thread = None # Será inicializada em start()

        self.received_messages_data = []
        self.response_times_ms_current_cycle = []
        self.message_counter_current_cycle = 0 
        self.total_messages_received_current_cycle = 0
        self.lock = threading.Lock()
        self.all_cycles_completed_event = threading.Event()
        self.experiment_results = []
        
        self.sample_texts_for_ia = [
            "Eu adorei este filme, os atores foram incríveis e a história me prendeu do início ao fim!",
            "Que decepção, esperava muito mais deste produto, não recomendo a ninguém.",
            "O atendimento no restaurante foi péssimo, demorou horas e a comida veio fria.",
            "Uma obra de arte cinematográfica, visualmente deslumbrante e emocionante.",
            "A experiência de compra online foi tranquila e o produto chegou antes do prazo.",
            "Não sei bem o que achar, teve partes boas e ruins, foi apenas mediano.",
            "Serviço de péssima qualidade, funcionários despreparados e ambiente sujo.",
            "Com certeza voltarei! A comida estava deliciosa e o ambiente muito agradável.",
            "Este livro me transportou para outro mundo, narrativa envolvente e personagens cativantes.",
            "Total perda de tempo e dinheiro, um dos piores que já vi.",
            "Os policiais agiram corretamente para conter a situação de perigo iminente.",
            "Houve um claro despreparo e uso excessivo de força na abordagem policial.",
            "A comunidade se sentiu mais segura após a intervenção das autoridades.",
            "É preciso investigar a fundo a conduta dos oficiais envolvidos nesse lamentável episódio.",
            "Ações como essa são necessárias para garantir a ordem e a tranquilidade pública.",
            "Inaceitável o nível de violência empregado, precisamos de uma polícia mais humana.",
            "Parabéns aos policiais pela bravura e dedicação em proteger os cidadãos.",
            "Os direitos humanos foram claramente violados durante aquela operação.",
            "A população local apoiou a ação da polícia contra a criminalidade na região.",
            "Mais uma vez, vemos a truculência policial se manifestar contra os mais pobres."
        ]

    def _send_config_message(self, num_services_to_configure):
        config_message = f"config;{num_services_to_configure};\n"
        max_retries = 3 
        attempt = 0
        while attempt < max_retries:
            attempt +=1
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as config_socket:
                    self.logger.info(f"Tentativa {attempt}/{max_retries}: Conectando a {self.config_target_lb_host}:{self.config_target_lb_port} para config...")
                    config_socket.connect((self.config_target_lb_host, self.config_target_lb_port))
                    config_socket.sendall(config_message.encode('utf-8'))
                    self.logger.info(f"Msg config '{config_message.strip()}' enviada.")
                    
                    config_socket.settimeout(25.0) # Timeout para LB confirmar que INICIOU a reconfig
                    response_bytes = config_socket.recv(1024)
                    if not response_bytes:
                        self.logger.warning(f"Tentativa {attempt}: LB fechou conexão sem resposta à config.")
                        if attempt < max_retries: time.sleep(15); continue
                        return False
                    response = response_bytes.decode('utf-8').strip()
                    self.logger.info(f"Tentativa {attempt}: Resposta da config do LB: {response}")
                    if "Configuration has finished" in response or "Configuration Alert" in response:
                        return True 
                    else:
                        self.logger.warning(f"Tentativa {attempt}: LB não confirmou config como esperado. Resposta: {response}")
                        if attempt < max_retries: time.sleep(15); continue
                        return False
            except socket.timeout:
                self.logger.error(f"Tentativa {attempt}: Timeout ({config_socket.gettimeout()}s) esperando resposta da config.")
            except ConnectionRefusedError:
                 self.logger.error(f"Tentativa {attempt}: Conexão recusada por {self.config_target_lb_host}:{self.config_target_lb_port}.")
            except Exception as e:
                self.logger.error(f"Tentativa {attempt}: Erro ao enviar/receber msg de config: {e}", exc_info=True)
            
            if attempt < max_retries: 
                self.logger.info(f"Aguardando 20s antes da próxima tentativa de enviar config...")
                time.sleep(20) 
        
        self.logger.error(f"Falha ao enviar/confirmar mensagem de config após {max_retries} tentativas.")
        return False

    def _send_message_to_lb1(self, cycle_id, message_id):
        timestamp_saida_source = time.time()
        text_for_ia = random.choice(self.sample_texts_for_ia)
        text_for_ia_safe = text_for_ia.replace(";", ",")
        message = f"{cycle_id};{message_id};{timestamp_saida_source:.6f};{text_for_ia_safe}"
        try:
            # É importante criar um novo socket para cada mensagem para evitar problemas
            # se o LB fechar a conexão após um ACK.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.target_lb1_host, self.target_lb1_port))
                s.sendall(message.encode('utf-8')) 
                self.logger.debug(f"Msg {cycle_id}-{message_id} enviada p/ LB1. Conteúdo: {message[:70]}...")
        except Exception as e:
            self.logger.error(f"Erro ao enviar msg {cycle_id}-{message_id} para LB1 ({self.target_lb1_host}:{self.target_lb1_port}): {e}")

    def _calculate_mrt_and_stddev(self, response_times_ms):
        if not response_times_ms: return 0.0, 0.0
        mean_mrt = statistics.mean(response_times_ms)
        std_dev = statistics.stdev(response_times_ms) if len(response_times_ms) > 1 else 0.0
        return mean_mrt, std_dev

    def _process_received_message(self, received_message_str):
        self.logger.debug(f"MSG COMPLETA RETORNADA AO SOURCE: {received_message_str.strip()}")
        parts = received_message_str.strip().split(';')
        
        # Formato esperado com Svc1 SEM IA e Svc2 COM IA (21 partes totais)
        # cID;mID;tsS0;TXT; L1in;dT1; L1f;dT2; S1in;dS1r; S1out;dT3encam; L2in;dL2r; L2f;dT4; S2in;dS2r; S2outIA;dT5IA; PRED_S2
        # 0  ;1  ;2   ;3  ; 4    ;5  ; 6  ;7  ; 8    ;9   ; 10    ;11      ; 12   ;13  ; 14  ;15 ; 16   ;17  ; 18      ;19    ; 20
        expected_num_parts = 21 

        try:
            if len(parts) < 3: 
                self.logger.error(f"Msg muito curta para processar MRT: {received_message_str.strip()}"); return
            
            cycle_id_str, msg_id_str, ts_saida_source_str = parts[0], parts[1], parts[2]
            original_text_sent = parts[3].replace(",", ";") if len(parts) > 3 else "N/A"
            ai_prediction_str = "N/A"; ai_prediction_label = "Desconhecida"

            if len(parts) >= expected_num_parts:
                ai_prediction_raw = parts[expected_num_parts - 1] 
                try:
                    if ai_prediction_raw not in ["-NO_IA-", "-ERR_IA-", "-PROC_FAIL-"]:
                        prediction_code = int(ai_prediction_raw)
                        if prediction_code == 1: ai_prediction_label = "aprova"
                        elif prediction_code == 0: ai_prediction_label = "neutro"
                        elif prediction_code == 2: ai_prediction_label = "desaprova"
                        else: ai_prediction_label = f"código desconhecido ({prediction_code})"
                        ai_prediction_str = ai_prediction_raw
                    else: 
                        ai_prediction_label = f"IA não executada/falhou ({ai_prediction_raw})"
                        ai_prediction_str = ai_prediction_raw
                except ValueError: 
                    ai_prediction_label = f"predição inválida ({ai_prediction_raw})"
                    ai_prediction_str = ai_prediction_raw
            else: 
                self.logger.warning(f"Msg formato inesperado (partes: {len(parts)}, esperado >= {expected_num_parts}): {received_message_str.strip()}")

            mrt_ms = (time.time() - float(ts_saida_source_str)) * 1000.0
            with self.lock:
                self.total_messages_received_current_cycle += 1
                self.response_times_ms_current_cycle.append(mrt_ms)
                self.received_messages_data.append({
                    'cycle_id': int(cycle_id_str), 'msg_id': int(msg_id_str), 'mrt_ms': mrt_ms, 
                    'full_message': received_message_str.strip(), 'original_text': original_text_sent,
                    'ai_prediction_code': ai_prediction_str, 'ai_prediction_label': ai_prediction_label
                })
                self.logger.info(f"Msg {cycle_id_str}-{msg_id_str} recebida. MRT: {mrt_ms:.3f} ms. Total ciclo: {self.total_messages_received_current_cycle}/{self.message_counter_current_cycle}")
                self.logger.info(f"  Texto: '{original_text_sent[:70]}...' Predição: {ai_prediction_label} ({ai_prediction_str})")
        except Exception as e: 
            self.logger.error(f"Erro ao processar msg '{received_message_str.strip()}': {e}", exc_info=True)

    def _listen_for_responses(self):
        self.logger.info(f"{self.source_name}_ListenerThread INICIADA e entrando no loop.")
        try:
            while not self.all_cycles_completed_event.is_set():
                try:
                    if not self.server_socket or self.server_socket.fileno() == -1:
                        self.logger.warning(f"{self.source_name}_ListenerThread: Socket do servidor parece fechado. Encerrando thread.")
                        break
                    self.server_socket.settimeout(1.0)
                    client_socket, address = self.server_socket.accept()
                    self.logger.info(f"{self.source_name}_ListenerThread: Conexão aceita de {address}")
                    try:
                        data_buffer = bytearray()
                        client_socket.settimeout(5.0) 
                        while True:
                            chunk = client_socket.recv(4096) 
                            if not chunk: break 
                            data_buffer.extend(chunk)
                            if data_buffer.endswith(b'\n'): break
                            if len(data_buffer) > 16384: self.logger.warning(f"Buffer excedeu 16KB para {address}."); break
                        if data_buffer: 
                            self.logger.debug(f"Dados recebidos de {address}: {len(data_buffer)} bytes")
                            self._process_received_message(data_buffer.decode('utf-8', errors='ignore'))
                    except socket.timeout: self.logger.warning(f"Timeout ao receber dados de {address} na listener_thread.")
                    except Exception as e_recv: self.logger.error(f"Erro ao receber dados de {address} na listener_thread: {e_recv}", exc_info=True)
                    finally: 
                        if client_socket: client_socket.close()
                except socket.timeout: continue 
                except OSError as e_os: 
                    if self.all_cycles_completed_event.is_set() or (self.server_socket and self.server_socket.fileno() == -1):
                        self.logger.info(f"{self.source_name}_ListenerThread: server_socket fechado (OSError), encerrando loop.")
                    else:
                        self.logger.error(f"{self.source_name}_ListenerThread: OSError no accept (socket possivelmente fechado): {e_os}")
                    break 
                except Exception as e_accept:
                    if not self.all_cycles_completed_event.is_set(): 
                        self.logger.error(f"{self.source_name}_ListenerThread: Erro inesperado no accept: {e_accept}", exc_info=True)
                    break 
        except Exception as e_outer:
             self.logger.error(f"{self.source_name}_ListenerThread: Erro crítico fora do loop principal: {e_outer}", exc_info=True)
        self.logger.info(f"{self.source_name}_ListenerThread TERMINANDO.")

    def run_experiment_cycles(self):
        # A listener_thread é iniciada no método start() agora
        
        initial_system_wait_time_sec = 75 
        self.logger.info(f"Source aguardando {initial_system_wait_time_sec}s para inicialização dos LBs e seus modelos...")
        time.sleep(initial_system_wait_time_sec)

        for cycle_idx, num_services_lb2 in enumerate(self.qtd_services_variation):
            self.logger.info(f"=== INICIANDO CICLO {cycle_idx + 1}/{len(self.qtd_services_variation)} com {num_services_lb2} serviço(s) no LB2 ===")
            if not self._send_config_message(num_services_lb2):
                self.logger.error(f"Falha definitiva ao enviar/confirmar config para LB2 ({num_services_lb2} svcs). Abortando ciclo."); continue
            
            lb_model_load_wait_sec = 30 
            self.logger.info(f"Aguardando LB2 reconfigurar e serviços de IA carregarem ({lb_model_load_wait_sec}s)...")
            time.sleep(lb_model_load_wait_sec) 

            with self.lock:
                self.response_times_ms_current_cycle.clear()
                self.message_counter_current_cycle = self.max_messages_per_cycle
                self.total_messages_received_current_cycle = 0
            
            self.logger.info(f"Iniciando envio de {self.max_messages_per_cycle} mensagens para o ciclo {cycle_idx+1}...")
            for msg_id_in_cycle in range(1, self.max_messages_per_cycle + 1):
                self._send_message_to_lb1(cycle_id=cycle_idx, message_id=msg_id_in_cycle)
                time.sleep(self.arrival_delay_sec)
            
            self.logger.info(f"Todas as {self.message_counter_current_cycle} msgs do ciclo {cycle_idx+1} enviadas. Aguardando respostas...")
            
            avg_ia_processing_time_estimation_sec = 45 
            buffer_time_sec = 60 
            cycle_timeout_seconds = (self.message_counter_current_cycle * avg_ia_processing_time_estimation_sec) + buffer_time_sec
            
            start_wait_time = time.time()
            while True:
                with self.lock:
                    if self.total_messages_received_current_cycle >= self.message_counter_current_cycle:
                        self.logger.info(f"Todas as {self.total_messages_received_current_cycle} respostas do ciclo {cycle_idx+1} recebidas."); break 
                if time.time() - start_wait_time > cycle_timeout_seconds:
                    self.logger.warning(f"Timeout ({cycle_timeout_seconds}s) esperando respostas para ciclo {cycle_idx+1}. Recebidas: {self.total_messages_received_current_cycle}/{self.message_counter_current_cycle}"); break 
                time.sleep(0.5)

            with self.lock:
                mrt_ciclo, std_dev_ciclo = self._calculate_mrt_and_stddev(self.response_times_ms_current_cycle)
                self.logger.info(f"=== RESULTADOS CICLO {cycle_idx + 1} ({num_services_lb2} svcs no LB2) ===")
                self.logger.info(f"Msgs Enviadas: {self.message_counter_current_cycle}, Recebidas: {self.total_messages_received_current_cycle}")
                self.logger.info(f"MRT: {mrt_ciclo:.3f} ms, Desvio Padrão: {std_dev_ciclo:.3f} ms")
                self.experiment_results.append({
                    'cycle_id': cycle_idx, 'qtd_services_lb2': num_services_lb2, 'mrt_ms': mrt_ciclo, 
                    'std_dev_ms': std_dev_ciclo, 'raw_mrts_ms': list(self.response_times_ms_current_cycle) 
                })
        
        self.all_cycles_completed_event.set()
        self.logger.info(f"Evento all_cycles_completed_event setado. Tentando join na listener_thread...")
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)
            if self.listener_thread.is_alive():
                self.logger.warning("Listener thread ainda viva após timeout do join.")
        elif self.listener_thread:
             self.logger.info("Listener thread não está viva antes do join (pode já ter terminado).")
        else:
            self.logger.warning("Atributo listener_thread não encontrado no Source para join.")

        try:
            if self.server_socket and self.server_socket.fileno() != -1:
                self.server_socket.close()
                self.logger.info("Socket de escuta do Source fechado ao final de run_experiment_cycles.")
        except Exception as e: 
            self.logger.error(f"Erro ao fechar socket do Source em run_experiment_cycles: {e}")

        self.logger.info("=== EXPERIMENTO COMPLETO ===")
        for result in self.experiment_results:
            self.logger.info(f"Ciclo {result['cycle_id']+1} (Svcs LB2: {result['qtd_services_lb2']}): MRT={result['mrt_ms']:.2f}ms, SD={result['std_dev_ms']:.2f}ms")
        self._log_mean_transition_times()

    def _log_mean_transition_times(self):
        self.logger.info("--- Tempos Intermediários Médios (Tx) ---")
        expected_delta_indices = {
            "T1 (Rede Src->LB1)": 5, "T2 (Fila LB1)": 7,
            "T3 (Svc1 Encaminhamento)": 11, "T4 (Fila LB2)": 15,
            "T5 (Svc2 IA)": 19
        }
        intermediate_times_map = {key: [] for key in expected_delta_indices}
        if not self.received_messages_data:
            for t_label in sorted(expected_delta_indices.keys()): self.logger.info(f"{t_label} = N/A"); return
        valid_msg_count = 0
        for msg_data in self.received_messages_data:
            parts = msg_data['full_message'].strip().split(';')
            if len(parts) < 21: continue
            valid_msg_count +=1
            for t_label, idx in expected_delta_indices.items():
                try: intermediate_times_map[t_label].append(float(parts[idx]))
                except: self.logger.warning(f"Erro ao extrair {t_label} da msg: {msg_data['full_message'][:70]}...")
        if valid_msg_count == 0: self.logger.warning("Nenhuma msg válida para calcular tempos Tx."); 
        for t_label, times in sorted(intermediate_times_map.items()):
            if times: self.logger.info(f"{t_label} = {statistics.mean(times):.3f} ms")
            else: self.logger.info(f"{t_label} = N/A")
    
    def start(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.listen_host, self.listen_port))
            self.server_socket.listen(10)
            self.logger.info(f"Socket do Source ouvindo por respostas em {self.listen_host}:{self.listen_port}")

            self.listener_thread = threading.Thread(target=self._listen_for_responses, name=f"{self.source_name}_ListenerThread")
            self.listener_thread.daemon = True # Daemon para não impedir o programa de sair
            self.listener_thread.start()
            self.logger.info(f"Listener thread iniciada para {self.source_name}.")
            
            self.run_experiment_cycles()

        except Exception as e:
            self.logger.critical(f"Erro CRÍTICO no método start do Source: {e}", exc_info=True)
        finally:
            self.logger.info(f"Source {self.source_name} encerrando.")
            # Garante que o evento seja setado para que a listener_thread saia do loop
            if not self.all_cycles_completed_event.is_set():
                self.all_cycles_completed_event.set()

            # Tenta fazer join na thread de escuta se ela foi iniciada
            if hasattr(self, 'listener_thread') and self.listener_thread and self.listener_thread.is_alive():
                self.logger.info("Aguardando listener_thread encerrar no finally do start...")
                self.listener_thread.join(timeout=2.0) # Curto timeout
                if self.listener_thread.is_alive():
                    self.logger.warning("Listener_thread do Source ainda ativa após join no finally.")
            
            # Fecha o socket do servidor se ele existe e está aberto
            if hasattr(self, 'server_socket') and self.server_socket:
                try:
                    if self.server_socket.fileno() != -1: # Verifica se o socket não está fechado
                        self.server_socket.close()
                        self.logger.info("Socket do Source fechado no finally do start.")
                except Exception as e_sock_close:
                    self.logger.error(f"Erro ao fechar socket do Source no finally: {e_sock_close}")

if __name__ == '__main__':
    main_logger_source_test = get_logger("SourceTestMain")
    main_logger_source_test.info("Iniciando teste individual do Source...")
    source_node = Source(
        listen_host='localhost', listen_port=4000, target_lb1_host='localhost', target_lb1_port=5000,
        arrival_delay_ms=3000, max_messages_per_cycle=2, qtd_services_variation=[1],
        config_target_lb_host='localhost', config_target_lb_port=6000, source_name="TestSource"
    )
    source_node.start() # Chama o método start que agora também inicia a listener_thread
    main_logger_source_test.info("Teste individual do Source concluído.")