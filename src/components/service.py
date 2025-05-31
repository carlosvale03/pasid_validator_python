import socket
import threading
import time
import random
from src.utils.logger_setup import get_logger
import os
import torch
from transformers import BertTokenizer, BertModel

class SentimentClassifier(torch.nn.Module):
    def __init__(self, n_classes, pre_trained_model_path_or_name='neuralmind/bert-large-portuguese-cased'):
        super(SentimentClassifier, self).__init__()
        logger_suffix = pre_trained_model_path_or_name.split('/')[-1].replace('-', '_')
        self.logger_sc = get_logger(f"SentimentClassifier_{logger_suffix}")
        self.logger_sc.info(f"SC: Inicializando BertModel a partir de: {pre_trained_model_path_or_name}")
        self.bert = BertModel.from_pretrained(pre_trained_model_path_or_name, return_dict=False)
        self.drop = torch.nn.Dropout(p=0.3)
        self.out = torch.nn.Linear(self.bert.config.hidden_size, n_classes)

    def forward(self, input_ids, attention_mask):
        _, pooled_output = self.bert(
            input_ids=input_ids,
            attention_mask=attention_mask
        )
        output = self.drop(pooled_output)
        return self.out(output)

class Service:
    def __init__(self, host, port, target_host, target_port,
                 ai_model_instance, tokenizer_instance,
                 is_target_source=False, service_name="Service",
                 max_len=180,
                 # Adicionando expected_to_have_ia
                 expected_to_have_ia: bool = False, 
                 service_time_mean=10, service_time_std_dev=2):

        self.host = host
        self.port = port
        self.service_name = service_name if f"_{port}" in service_name else f"{service_name}_{port}"
        self.logger = get_logger(self.service_name)

        self.target_host = target_host
        self.target_port = target_port
        self.is_target_source = is_target_source
        
        self.processing_lock = threading.Lock()
        self.is_busy = False

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.logger.info(f"Ouvindo em {self.host}:{self.port}")

        self.ai_model = ai_model_instance
        self.tokenizer = tokenizer_instance
        self.max_len = max_len
        self.expected_to_have_ia = expected_to_have_ia # Armazena a expectativa

        if self.ai_model:
            try:
                self.device = next(self.ai_model.parameters()).device
            except StopIteration: 
                self.logger.warning("Modelo AI fornecido não tem parâmetros, usando fallback para device CPU/CUDA.")
                self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            except Exception as e_dev:
                 self.logger.warning(f"Não foi possível inferir o device do modelo AI: {e_dev}. Usando fallback.")
                 self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        self.service_time_mean_ms_if_no_ia = service_time_mean 
        self.service_time_std_dev_ms_if_no_ia = service_time_std_dev

        if self.ai_model and self.tokenizer:
            self.logger.info(f"Instância de IA e Tokenizer recebida. Usando dispositivo: {self.device}.")
        elif self.expected_to_have_ia and (not self.ai_model or not self.tokenizer):
            self.logger.error(f"{self.service_name} ESPERAVA ter IA, mas não recebeu modelo/tokenizer válidos.")
        else: # Não esperava IA e não recebeu, ou esperava e recebeu (já coberto acima)
            self.logger.info(f"{self.service_name} configurado SEM IA (ou IA não pôde ser inicializada). Atuará como encaminhador/simulador.")
        
        self.example_processing_texts = ["Texto de fallback se necessário."] * 5
    
    # ... (métodos _register_time, _perform_ia_processing, _process_and_forward_message_core, handle_incoming_connection permanecem como na última versão ID N1mD8q)
    def _register_time(self, message_parts):
        current_time = time.time()
        last_timestamp_str = "N/A"; last_timestamp = current_time; time_since_last = 0.0
        try:
            if not message_parts: self.logger.warning("_register_time: message_parts está vazio."); return [f"{current_time:.6f}", "0.000"]
            relevant_timestamp_index = -1
            if len(message_parts) == 4 and all(c.isdigit() or c == '.' for c in message_parts[2]): relevant_timestamp_index = 2
            elif len(message_parts) > 2 and all(c.isdigit() or c == '.' for c in message_parts[-2]): relevant_timestamp_index = -2
            else: self.logger.debug(f"_register_time: Não foi possível determinar último TS. Parts: {message_parts}")
            if relevant_timestamp_index != -1 and -len(message_parts) <= relevant_timestamp_index < len(message_parts) :
                 last_timestamp_str = message_parts[relevant_timestamp_index]; last_timestamp = float(last_timestamp_str)
                 time_since_last = (current_time - last_timestamp) * 1000
        except ValueError: self.logger.error(f"_register_time: ERRO ao converter '{last_timestamp_str}'. Parts: {message_parts}")
        except IndexError: self.logger.error(f"_register_time: ERRO índice. Parts: {message_parts}")
        message_parts.append(f"{current_time:.6f}"); message_parts.append(f"{time_since_last:.3f}")
        return message_parts

    def _perform_ia_processing(self, text_to_process_list):
        if not self.ai_model or not self.tokenizer:
            simulated_delay_ms = random.gauss(self.service_time_mean_ms_if_no_ia, self.service_time_std_dev_ms_if_no_ia)
            if simulated_delay_ms < 0: simulated_delay_ms = 0
            time.sleep(simulated_delay_ms / 1000.0)
            self.logger.debug(f"{self.service_name} (sem IA): Simulado delay de encaminhamento de {simulated_delay_ms:.3f} ms.")
            return simulated_delay_ms, ["-NO_IA-"] * len(text_to_process_list) 
        if not text_to_process_list: self.logger.warning(f"Nenhum texto para processamento IA em {self.service_name}."); return 0.0, [] 
        if not isinstance(text_to_process_list, list): text_to_process_list = [text_to_process_list]
        self.logger.debug(f"Iniciando processamento IA em {self.service_name} para {len(text_to_process_list)} texto(s). Ex: '{text_to_process_list[0][:30]}...'")
        start_time_ia = time.time(); predictions = []
        try:
            encoding = self.tokenizer.batch_encode_plus(
                text_to_process_list, add_special_tokens=True, max_length=self.max_len,
                return_token_type_ids=False, padding='max_length', 
                truncation=True, return_attention_mask=True, return_tensors='pt')
            input_ids = encoding['input_ids'].to(self.device); attention_mask = encoding['attention_mask'].to(self.device)
            with torch.no_grad(): 
                outputs = self.ai_model(input_ids, attention_mask)
                _, preds_tensor = torch.max(outputs, dim=1)
                predictions = preds_tensor.cpu().tolist()
        except Exception as e:
            self.logger.error(f"Erro na predição da IA em {self.service_name}: {e}", exc_info=True)
            return (time.time() - start_time_ia) * 1000.0, ["-ERR_IA-"] * len(text_to_process_list) 
        processing_time_ms = (time.time() - start_time_ia) * 1000.0
        self.logger.debug(f"IA concluída em {self.service_name}. Duração: {processing_time_ms:.3f} ms. Predições: {predictions}")
        return processing_time_ms, predictions

    def _process_and_forward_message_core(self, message_str):
        message_parts = message_str.strip().split(';')
        msg_id_log = "_".join(message_parts[:2]) if len(message_parts) >= 2 else "MSG_ID_DESCONHECIDO"
        text_payload_for_ia = "" 
        index_texto_ia = 3 
        if len(message_parts) > index_texto_ia: text_payload_for_ia = message_parts[index_texto_ia]
        else: self.logger.warning(f"Msg '{msg_id_log}' sem payload de texto no índice {index_texto_ia}. Usando fallback."); text_payload_for_ia = random.choice(self.example_processing_texts) 
        message_parts = self._register_time(message_parts) 
        timestamp_antes_proc = time.time()
        tempo_proc_ms, proc_outputs = self._perform_ia_processing([text_payload_for_ia])
        output_val_str = str(proc_outputs[0]) if proc_outputs else "-PROC_FAIL-"
        ts_saida_processamento = timestamp_antes_proc + (tempo_proc_ms / 1000.0)
        message_parts.append(f"{ts_saida_processamento:.6f}"); message_parts.append(f"{tempo_proc_ms:.3f}")
        if self.ai_model and self.tokenizer: 
            message_parts.append(output_val_str) 
            self.logger.info(f"Msg '{msg_id_log}' processada (IA previu: {output_val_str}). Enviando para {self.target_host}:{self.target_port}...")
        else: self.logger.info(f"Msg '{msg_id_log}' encaminhada (sem IA por este serviço). Enviando para {self.target_host}:{self.target_port}...")
        processed_message_str = ";".join(message_parts)
        self.logger.debug(f"Conteúdo msg processada/encaminhada: {processed_message_str}")
        max_retries = 3; sent_successfully = False
        for attempt in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket_to_target:
                    client_socket_to_target.connect((self.target_host, self.target_port))
                    client_socket_to_target.sendall((processed_message_str + "\n").encode('utf-8'))                
                    if not self.is_target_source: 
                        client_socket_to_target.settimeout(5.0) 
                        ack_target = client_socket_to_target.recv(1024).decode('utf-8')
                        if ack_target.strip() == "ack_queued": self.logger.debug(f"Msg '{msg_id_log}' enviada para LB e ACK_QUEUED recebido.")
                        else: self.logger.warning(f"Alvo LB {self.target_host}:{self.target_port} respondeu '{ack_target}'.")
                    else: self.logger.debug(f"Msg '{msg_id_log}' enviada para Source.")
                    sent_successfully = True; break 
            except socket.timeout: self.logger.warning(f"Timeout ACK de {self.target_host}:{self.target_port} (tentativa {attempt+1}).")
            except Exception as e: self.logger.error(f"Falha ao enviar para {self.target_host}:{self.target_port} (tentativa {attempt+1}): {e}")
            if not sent_successfully and attempt < max_retries - 1: time.sleep(0.2 * (attempt + 1))
            elif not sent_successfully: self.logger.critical(f"NÃO FOI POSSÍVEL enviar msg '{msg_id_log}' para {self.target_host}:{self.target_port}.")

    def handle_incoming_connection(self, client_socket, address):
        conn_thread_name = threading.current_thread().name 
        self.logger.info(f"[{conn_thread_name}] Conexão aceita de {address}")
        try:
            data = client_socket.recv(4096) 
            if not data: self.logger.debug(f"[{conn_thread_name}] Nenhum dado de {address}."); return
            message_str = data.decode('utf-8').strip()
            if message_str.lower() == "ping":
                with self.processing_lock: status_val = self.is_busy 
                status_msg = b"busy" if status_val else b"free"
                client_socket.sendall(status_msg)
                self.logger.debug(f"[{conn_thread_name}] Ping de {address} respondido com: {status_msg.decode()}")
            else:
                self.logger.debug(f"[{conn_thread_name}] Mensagem de dados de {address}: {message_str[:70]}...")
                can_process = False
                with self.processing_lock:
                    if not self.is_busy: self.is_busy = True; can_process = True
                if can_process:
                    self.logger.debug(f"[{conn_thread_name}] Status 'is_busy' ATIVADO. Processando dados de {address}.")
                    try:
                        msg_id_log_parts = message_str.split(';', 2)
                        msg_id_log_display = "_".join(msg_id_log_parts[:2]) if len(msg_id_log_parts) >=2 else "MSG_DESC"
                        self.logger.debug(f"[{conn_thread_name}] Enviando ACK_RECEIVED para {address} por msg '{msg_id_log_display}'")
                        client_socket.sendall("ack_received".encode('utf-8'))
                        client_socket.close(); client_socket = None 
                        self.logger.debug(f"[{conn_thread_name}] Conexão com LB {address} fechada após ACK_RECEIVED.")
                        self._process_and_forward_message_core(message_str) 
                    finally:
                        with self.processing_lock: self.is_busy = False
                        self.logger.debug(f"[{conn_thread_name}] Status 'is_busy' DESATIVADO.")
                else: 
                    self.logger.warning(f"[{conn_thread_name}] Serviço já 'busy', respondendo 'busy_processing' para {address}.")
                    client_socket.sendall("busy_processing".encode('utf-8'))
        except socket.timeout: self.logger.warning(f"[{conn_thread_name}] Timeout na conexão com {address}.")
        except Exception as e: self.logger.error(f"[{conn_thread_name}] Erro ao lidar com {address}: {e}", exc_info=True)
        finally:
            if client_socket: 
                try: client_socket.close()
                except Exception: pass

    def start(self):
        threading.current_thread().name = f"{self.service_name}_AcceptThread"
        
        service_ready = True
        if self.expected_to_have_ia: # Se este serviço deveria ter IA
            if not self.ai_model or not self.tokenizer:
                self.logger.critical(f"Modelo de IA ou Tokenizer NÃO FORNECIDO para {self.service_name} que ESPERAVA IA. Serviço NÃO FUNCIONARÁ COM IA.")
                service_ready = False # Indica que não está pronto para operar como IA
            else:
                self.logger.info(f"{self.service_name} iniciado e pronto (com IA).")
        else: # Não se esperava IA
            self.logger.info(f"{self.service_name} iniciado e pronto (SEM IA - modo encaminhador/simulador).")

        # Mesmo que a IA esperada não tenha sido fornecida, o serviço pode tentar iniciar o socket de escuta
        # mas o _perform_ia_processing irá simular/retornar erro.
        # Se for crítico que um serviço de IA não inicie sem IA, a lógica de _create_service_instance no LB já previne isso.
        # A verificação aqui é mais para logar o estado.
        
        while True:
            try:
                client_socket, address = self.server_socket.accept()
                conn_handler_thread = threading.Thread(target=self.handle_incoming_connection, args=(client_socket, address), name=f"{self.service_name}_ConnHandler_{address[1]}")
                conn_handler_thread.daemon = True; conn_handler_thread.start()
            except OSError: self.logger.info(f"Socket de escuta fechado para {self.service_name}. Encerrando."); break
            except Exception as e: self.logger.critical(f"Erro ao aceitar conexão em {self.service_name}: {e}. Tentando continuar.", exc_info=True); time.sleep(0.1) 
        
        if hasattr(self, 'server_socket') and self.server_socket:
            try: self.server_socket.close(); self.logger.info(f"Socket do servidor de {self.service_name} fechado.")
            except Exception as e: self.logger.error(f"Erro ao fechar socket do {self.service_name}: {e}")

# Bloco if __name__ == '__main__' como na sua última versão (ID N1mD8q), 
# ele já está adaptado para testar com e sem IA passando os parâmetros corretos.
if __name__ == '__main__':
    main_logger_svc_test = get_logger("ServiceTestStandalone")
    CAMINHO_MODELO_HOST_TEST = r"D:/IC/youtube_model_1124" 
    NOME_MODELO_HF_TEST = 'neuralmind/bert-large-portuguese-cased'
    
    main_logger_svc_test.info("Testando Service COM IA...")
    bin_path_test1 = os.path.join(CAMINHO_MODELO_HOST_TEST, 'models', 'posicionamento_BERTimabauPT-BR_211124.bin')
    bin_path_test2 = os.path.join(CAMINHO_MODELO_HOST_TEST, 'posicionamento_BERTimabauPT-BR_211124.bin')
    if not (os.path.exists(bin_path_test1) or os.path.exists(bin_path_test2)):
        main_logger_svc_test.error(f"Arquivo .bin não encontrado em {CAMINHO_MODELO_HOST_TEST}. Teste COM IA abortado.")
    else:
        try:
            tokenizer_test_instance = BertTokenizer.from_pretrained(NOME_MODELO_HF_TEST)
            ai_model_test_instance = SentimentClassifier(3, pre_trained_model_path_or_name=NOME_MODELO_HF_TEST)
            
            dict_path = bin_path_test1 if os.path.exists(bin_path_test1) else bin_path_test2
            ai_model_test_instance.load_state_dict(torch.load(dict_path, map_location=torch.device('cpu')))
            ai_model_test_instance.eval()
            main_logger_svc_test.info("Modelo e Tokenizer de teste carregados para o Service COM IA.")

            service_node_with_ia = Service(
                host='localhost', port=7001, target_host='localhost', target_port=4000, 
                ai_model_instance=ai_model_test_instance, 
                tokenizer_instance=tokenizer_test_instance,
                is_target_source=True, service_name="TestServiceWithIA",
                expected_to_have_ia=True # Para o teste, este serviço espera IA
            )
            if service_node_with_ia.ai_model: 
                main_logger_svc_test.info("Nó de serviço com IA pronto para teste (start manual necessário).")
                # service_node_with_ia.start() # Descomente para rodar
            else: main_logger_svc_test.error("Falha ao preparar nó de serviço com IA.")
        except Exception as e: main_logger_svc_test.error(f"Erro ao preparar teste COM IA: {e}", exc_info=True)

    main_logger_svc_test.info("\nTestando Service SEM IA (modo encaminhador)...")
    service_node_no_ia = Service(
        host='localhost', port=7002, target_host='localhost', target_port=4000, 
        ai_model_instance=None, 
        tokenizer_instance=None,
        is_target_source=True, service_name="TestServiceNoIA",
        expected_to_have_ia=False, # Para o teste, este serviço NÃO espera IA
        service_time_mean=50, service_time_std_dev=5 
    )
    main_logger_svc_test.info("Nó de serviço SEM IA pronto para teste (start manual necessário).")
    # service_node_no_ia.start() # Descomente para rodar