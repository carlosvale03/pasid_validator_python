import threading
import time
import argparse
import os
from src.components.source import Source
from src.components.load_balancer import LoadBalancer
from src.utils.logger_setup import get_logger

main_logger = get_logger("MainOrchestrator")

# --- CONFIGURAÇÕES GERAIS ---
SOURCE_LISTEN_PORT = 4000
LB1_LISTEN_PORT = 5000
LB2_LISTEN_PORT = 6000

CAMINHO_MODELO_BERT_HOST = "D:/IC/youtube_model_1124"
CAMINHO_MODELO_BERT_CONTAINER = "/app/bert_model_data"

# Configurações para Docker
SOURCE_CONFIG_DOCKER = {
    'listen_host': '0.0.0.0',
    'listen_port': SOURCE_LISTEN_PORT,
    'target_lb1_host': 'lb1',
    'target_lb1_port': LB1_LISTEN_PORT,
    'arrival_delay_ms': 1000, # (ex: 2000, 1000, 500, 250, 100, 50)
    'max_messages_per_cycle': 30,
    'qtd_services_variation': [1, 2],
    'config_target_lb_host': 'lb2',
    'config_target_lb_port': LB2_LISTEN_PORT,
    'source_name': "SourceService"
}

SERVICE_CONFIG_BASE_IA_TEMPLATE = {
    'bert_model_name': 'neuralmind/bert-large-portuguese-cased',
    'num_classes': 3,
    'max_len': 180,
    'time_mean': 0,
    'time_std_dev': 0,
}

# Config para serviços do LB1 (SEM IA no Docker)
SERVICE_CONFIG_FOR_LB1_DOCKER = {
    **SERVICE_CONFIG_BASE_IA_TEMPLATE, # Herda num_classes, max_len, etc.
    'host': '0.0.0.0',
    'model_path': None, # LB1 não lida com o .bin
    'bert_model_name': None, # LB1 não carrega modelo base
    # 'model_path': CAMINHO_MODELO_BERT_CONTAINER, # Para o .bin
    # 'bert_model_name': 'neuralmind/bert-large-portuguese-cased', # Para config/tokenizer base
    'has_ia': False,
    'time_mean': 100,
    'time_std_dev': 10,
    'target_host': 'lb2',
    'target_port': LB2_LISTEN_PORT,
    'target_is_source': False
}
LB1_CONFIG_DOCKER = {
    'host': '0.0.0.0',
    'port': LB1_LISTEN_PORT,
    'max_queue_size': 20,
    'initial_num_services': 2,
    'service_config': SERVICE_CONFIG_FOR_LB1_DOCKER,
    'lb_name': "LoadBalancer1Service"
}

# Config para serviços do LB2 (COM IA no Docker)
SERVICE_CONFIG_FOR_LB2_DOCKER = {
    **SERVICE_CONFIG_BASE_IA_TEMPLATE,
    'host': '0.0.0.0',
    'model_path': CAMINHO_MODELO_BERT_CONTAINER, # Para o .bin
    'bert_model_name': 'neuralmind/bert-large-portuguese-cased', # Para config/tokenizer base
    'has_ia': True,
    'target_host': 'source',
    'target_port': SOURCE_LISTEN_PORT,
    'target_is_source': True
}
LB2_CONFIG_DOCKER = {
    'host': '0.0.0.0',
    'port': LB2_LISTEN_PORT,
    'max_queue_size': 30,
    'service_config': SERVICE_CONFIG_FOR_LB2_DOCKER,
    'lb_name': "LoadBalancer2Service"
    # initial_num_services será definido com base na config do Source
}

# --- Configurações para teste local "all" ---
SOURCE_CONFIG_LOCAL_ALL = SOURCE_CONFIG_DOCKER.copy()
SOURCE_CONFIG_LOCAL_ALL['listen_host'] = 'localhost'
SOURCE_CONFIG_LOCAL_ALL['target_lb1_host'] = 'localhost'
SOURCE_CONFIG_LOCAL_ALL['config_target_lb_host'] = 'localhost'
SOURCE_CONFIG_LOCAL_ALL['source_name'] = "MainSourceLocal"

# Config para serviços do LB1 (SEM IA localmente)
SERVICE_CONFIG_FOR_LB1_LOCAL = {
    **SERVICE_CONFIG_BASE_IA_TEMPLATE,
    'host': 'localhost',
    'model_path': None,
    'bert_model_name': None,
    'has_ia': False,
    'time_mean': 5,
    'time_std_dev': 1,
    'target_host': 'localhost',
    'target_port': LB2_LISTEN_PORT,
    'target_is_source': False
}
LB1_CONFIG_LOCAL_ALL = LB1_CONFIG_DOCKER.copy() # Herda algumas chaves do Docker config
LB1_CONFIG_LOCAL_ALL['host'] = 'localhost'
LB1_CONFIG_LOCAL_ALL['service_config'] = SERVICE_CONFIG_FOR_LB1_LOCAL
LB1_CONFIG_LOCAL_ALL['lb_name'] = "LoadBalancer1Local"

# Config para serviços do LB2 (COM IA localmente)
SERVICE_CONFIG_FOR_LB2_LOCAL = {
    **SERVICE_CONFIG_BASE_IA_TEMPLATE,
    'host': 'localhost',
    'model_path': CAMINHO_MODELO_BERT_HOST, # Para o .bin
    'bert_model_name': 'neuralmind/bert-large-portuguese-cased', # Para config/tokenizer base
    'has_ia': True,
    'target_host': 'localhost',
    'target_port': SOURCE_LISTEN_PORT,
    'target_is_source': True
}
LB2_CONFIG_LOCAL_ALL = LB2_CONFIG_DOCKER.copy() # Herda algumas chaves do Docker config
LB2_CONFIG_LOCAL_ALL['host'] = 'localhost'
LB2_CONFIG_LOCAL_ALL['service_config'] = SERVICE_CONFIG_FOR_LB2_LOCAL
LB2_CONFIG_LOCAL_ALL['lb_name'] = "LoadBalancer2Local"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PASID-VALIDATOR Component Runner")
    parser.add_argument("--component", choices=["source", "lb1", "lb2", "all"], default="all", help="Component to run")
    parser.add_argument("--env", choices=["local", "docker"], default="local", help="Execution environment")
    args = parser.parse_args()

    main_logger.info(f"Iniciando sistema PASID-VALIDATOR (componente: {args.component}, ambiente: {args.env})...")
    
    active_source_config = {}
    active_lb1_config = {}
    active_lb2_config = {}
    effective_model_path_for_bin = "" 

    if args.env == "docker":
        main_logger.info("--- Carregando configurações para ambiente DOCKER ---")
        effective_model_path_for_bin = CAMINHO_MODELO_BERT_CONTAINER
        main_logger.info(f"Caminho para state_dict (.bin) DENTRO DO CONTAINER: {effective_model_path_for_bin}")

        active_source_config = SOURCE_CONFIG_DOCKER.copy()
        
        active_lb1_config = LB1_CONFIG_DOCKER.copy()
        # service_config para LB1 já está correta (SERVICE_CONFIG_FOR_LB1_DOCKER), sem IA explícita.
        # Se service_config precisasse de model_path dinâmico (mesmo que None), seria aqui.
        # No caso atual, SERVICE_CONFIG_FOR_LB1_DOCKER já tem model_path: None e has_ia: False.

        active_lb2_config = LB2_CONFIG_DOCKER.copy()
        # service_config para LB2 já está correta (SERVICE_CONFIG_FOR_LB2_DOCKER), com IA e model_path.
        active_lb2_config['service_config']['model_path'] = effective_model_path_for_bin # Confirma o caminho do .bin
        
        if active_source_config.get('qtd_services_variation'):
            active_lb2_config['initial_num_services'] = active_source_config['qtd_services_variation'][0]
        else:
            active_lb2_config['initial_num_services'] = 1
            main_logger.warning("qtd_services_variation vazia na config Docker. Usando 1 serviço inicial para LB2.")

    else: # args.env == "local"
        main_logger.info("--- Carregando configurações para ambiente LOCAL ---")
        effective_model_path_for_bin = CAMINHO_MODELO_BERT_HOST
        main_logger.info(f"Caminho para state_dict (.bin) NO HOST: {effective_model_path_for_bin}")
        
        if args.component != "source": 
             if not os.path.exists(effective_model_path_for_bin):
                 main_logger.critical(f"Caminho base do modelo BERT no host ({effective_model_path_for_bin}) não encontrado. Verifique CAMINHO_MODELO_BERT_HOST.")
                 if args.component != "all": exit(1)
        
        active_source_config = SOURCE_CONFIG_LOCAL_ALL.copy()
        active_lb1_config = LB1_CONFIG_LOCAL_ALL.copy() 
        active_lb2_config = LB2_CONFIG_LOCAL_ALL.copy() 
        if active_source_config.get('qtd_services_variation'):
            active_lb2_config['initial_num_services'] = active_source_config['qtd_services_variation'][0]
        else:
            active_lb2_config['initial_num_services'] = 1
            main_logger.warning("qtd_services_variation está vazia na config Local. Usando 1 serviço inicial para LB2.")

    main_logger.info(f"Caminho efetivo para o .bin do modelo (se aplicável ao componente): {effective_model_path_for_bin}")

    if args.component == "source":
        main_logger.info(f"--- Iniciando SOMENTE Source ({active_source_config.get('source_name', 'SourceDefault')}) ---")
        source_node = Source(**active_source_config)
        source_thread = threading.Thread(target=source_node.start, name=f"{active_source_config.get('source_name', 'SourceDefault')}_ExperimentThread")
        source_thread.daemon = False; source_thread.start(); source_thread.join()

    elif args.component == "lb1":
        main_logger.info(f"--- Iniciando SOMENTE LoadBalancer 1 ({active_lb1_config.get('lb_name', 'LB1Default')}) ---")
        lb1 = LoadBalancer(**active_lb1_config)
        lb1_thread = threading.Thread(target=lb1.start, name=f"{active_lb1_config.get('lb_name', 'LB1Default')}_MainThread")
        lb1_thread.daemon = False; lb1_thread.start(); lb1_thread.join()

    elif args.component == "lb2":
        main_logger.info(f"--- Iniciando SOMENTE LoadBalancer 2 ({active_lb2_config.get('lb_name', 'LB2Default')}) ---")
        lb2 = LoadBalancer(**active_lb2_config)
        lb2_thread = threading.Thread(target=lb2.start, name=f"{active_lb2_config.get('lb_name', 'LB2Default')}_MainThread")
        lb2_thread.daemon = False; lb2_thread.start(); lb2_thread.join()

    elif args.component == "all":
        if args.env == "docker":
            main_logger.error("Modo 'all' é para execução local. Para Docker, use docker-compose.")
            exit(1)
        
        main_logger.info("--- Configurando TODOS os componentes para execução conjunta (modo local 'all') ---")
        current_source_config = active_source_config
        current_lb1_config = active_lb1_config
        current_lb2_config = active_lb2_config
        
        main_logger.info(f"--- (All Mode) Configurando LoadBalancer 2 ({current_lb2_config.get('lb_name', 'LB2Default')}) ---")
        lb2_instance = LoadBalancer(**current_lb2_config)
        lb2_thread_all = threading.Thread(target=lb2_instance.start, name=f"{current_lb2_config.get('lb_name', 'LB2Default')}_MainThread_All")
        lb2_thread_all.daemon = True

        main_logger.info(f"--- (All Mode) Configurando LoadBalancer 1 ({current_lb1_config.get('lb_name', 'LB1Default')}) ---")
        lb1_instance = LoadBalancer(**current_lb1_config)
        lb1_thread_all = threading.Thread(target=lb1_instance.start, name=f"{current_lb1_config.get('lb_name', 'LB1Default')}_MainThread_All")
        lb1_thread_all.daemon = True
        
        main_logger.info("Aguardando LoadBalancers iniciarem (modelo IA será carregado apenas no LB2 se configurado)...")
        lb2_thread_all.start(); main_logger.info(f"{current_lb2_config.get('lb_name', 'LB2Default')} thread iniciada.")
        time.sleep(2) 
        lb1_thread_all.start(); main_logger.info(f"{current_lb1_config.get('lb_name', 'LB1Default')} thread iniciada.")
        
        # Tempo de espera para carregamento do modelo no LB2 (se tiver IA)
        # Ajuste este tempo se o carregamento do modelo no LB2 for demorado
        model_load_wait_time_for_lb2 = 30 
        if current_lb2_config['service_config'].get('has_ia', False):
            main_logger.info(f"Aguardando {model_load_wait_time_for_lb2}s para modelo de IA no LB2 carregar...")
            time.sleep(model_load_wait_time_for_lb2) 
        else:
            main_logger.info("LB2 configurado sem IA, espera menor para inicialização.")
            time.sleep(5) # Espera mais curta se LB2 não carrega modelo pesado

        main_logger.info(f"--- (All Mode) Configurando Source ({current_source_config.get('source_name', 'SourceDefault')}) ---")
        source_node_all = Source(**current_source_config)
        source_thread_all = threading.Thread(target=source_node_all.start, name=f"{current_source_config.get('source_name', 'SourceDefault')}_ExperimentThread_All")
        source_thread_all.daemon = False 
        source_thread_all.start()
        main_logger.info(f"{current_source_config.get('source_name', 'SourceDefault')} thread iniciada.")
        source_thread_all.join() 
        main_logger.info("--- Simulação PASID-VALIDATOR Concluída (modo 'all') ---")
    
    main_logger.info("Programa principal encerrado.")