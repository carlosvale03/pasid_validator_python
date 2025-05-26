import threading
import time
from src.components.source import Source
from src.components.load_balancer import LoadBalancer
from src.utils.logger_setup import get_logger

# Obtém um logger para o script principal de orquestração
main_logger = get_logger("MainOrchestrator")

# --- CONFIGURAÇÕES GERAIS ---
# Source -> LB1 -> Services do LB1 -> LB2 -> Services do LB2 -> Source

# Portas dos componentes principais
SOURCE_LISTEN_PORT = 4000
LB1_LISTEN_PORT = 5000
LB2_LISTEN_PORT = 6000

# Configurações do Source
SOURCE_CONFIG = {
    'listen_host': 'localhost',
    'listen_port': SOURCE_LISTEN_PORT,
    'target_lb1_host': 'localhost',
    'target_lb1_port': LB1_LISTEN_PORT,
    'arrival_delay_ms': 200,
    'max_messages_per_cycle': 5,
    'qtd_services_variation': [1, 2],
    'config_target_lb_host': 'localhost',
    'config_target_lb_port': LB2_LISTEN_PORT,
    'source_name': "MainSource"
}

# Configurações para os SERVIÇOS gerenciados pelo LoadBalancer 1
SERVICE_CONFIG_FOR_LB1 = {
    'host': 'localhost',
    'target_host': 'localhost',
    'target_port': LB2_LISTEN_PORT,
    'time_mean': 0.05, # 50ms
    'time_std_dev': 0.01,
    'target_is_source': False
}

# Configurações do LoadBalancer 1
LB1_CONFIG = {
    'host': 'localhost',
    'port': LB1_LISTEN_PORT,
    'max_queue_size': 10,
    'initial_num_services': 1,
    'service_config': SERVICE_CONFIG_FOR_LB1,
    'lb_name': "LoadBalancer1"
}

# Configurações para os SERVIÇOS gerenciados pelo LoadBalancer 2
SERVICE_CONFIG_FOR_LB2 = {
    'host': 'localhost',
    'target_host': 'localhost',
    'target_port': SOURCE_LISTEN_PORT,
    'time_mean': 0.1, # 100ms
    'time_std_dev': 0.02,
    'target_is_source': True
}

# Configurações do LoadBalancer 2
LB2_CONFIG = {
    'host': 'localhost',
    'port': LB2_LISTEN_PORT,
    'max_queue_size': 10,
    'initial_num_services': SOURCE_CONFIG['qtd_services_variation'][0],
    'service_config': SERVICE_CONFIG_FOR_LB2,
    'lb_name': "LoadBalancer2"
}


if __name__ == '__main__':
    print("Iniciando o sistema PASID-VALIDATOR em Python...")
    main_logger.info("Iniciando o sistema PASID-VALIDATOR em Python...")

    threads = []

    main_logger.info(f"--- Configurando LoadBalancer 2 ({LB2_CONFIG['lb_name']}) ---")
    lb2 = LoadBalancer(
        host=LB2_CONFIG['host'],
        port=LB2_CONFIG['port'],
        max_queue_size=LB2_CONFIG['max_queue_size'],
        initial_num_services=LB2_CONFIG['initial_num_services'],
        service_config=LB2_CONFIG['service_config'],
        lb_name=LB2_CONFIG['lb_name']
    )
    # O nome da thread principal do LB (AcceptThread) é definido dentro do start do LB
    lb2_thread = threading.Thread(target=lb2.start, name=f"{LB2_CONFIG['lb_name']}_MainThread")
    lb2_thread.daemon = True
    threads.append(lb2_thread)
    main_logger.info(f"Thread para {LB2_CONFIG['lb_name']} criada.")

    main_logger.info(f"--- Configurando LoadBalancer 1 ({LB1_CONFIG['lb_name']}) ---")
    lb1 = LoadBalancer(
        host=LB1_CONFIG['host'],
        port=LB1_CONFIG['port'],
        max_queue_size=LB1_CONFIG['max_queue_size'],
        initial_num_services=LB1_CONFIG['initial_num_services'],
        service_config=LB1_CONFIG['service_config'],
        lb_name=LB1_CONFIG['lb_name']
    )
    lb1_thread = threading.Thread(target=lb1.start, name=f"{LB1_CONFIG['lb_name']}_MainThread")
    lb1_thread.daemon = True
    threads.append(lb1_thread)
    main_logger.info(f"Thread para {LB1_CONFIG['lb_name']} criada.")

    main_logger.info("Aguardando LoadBalancers iniciarem (3 segundos)...")
    lb2_thread.start()
    main_logger.info(f"{LB2_CONFIG['lb_name']} thread iniciada.")
    lb1_thread.start()
    main_logger.info(f"{LB1_CONFIG['lb_name']} thread iniciada.")
    time.sleep(3)

    main_logger.info(f"--- Configurando Source ({SOURCE_CONFIG['source_name']}) ---")
    source_node = Source(
        listen_host=SOURCE_CONFIG['listen_host'],
        listen_port=SOURCE_CONFIG['listen_port'],
        target_lb1_host=SOURCE_CONFIG['target_lb1_host'],
        target_lb1_port=SOURCE_CONFIG['target_lb1_port'],
        arrival_delay_ms=SOURCE_CONFIG['arrival_delay_ms'],
        max_messages_per_cycle=SOURCE_CONFIG['max_messages_per_cycle'],
        qtd_services_variation=SOURCE_CONFIG['qtd_services_variation'],
        config_target_lb_host=SOURCE_CONFIG['config_target_lb_host'],
        config_target_lb_port=SOURCE_CONFIG['config_target_lb_port'],
        source_name=SOURCE_CONFIG['source_name']
    )
    source_thread = threading.Thread(target=source_node.start, name=f"{SOURCE_CONFIG['source_name']}_ExperimentThread")
    # source_thread.daemon = True # Se False, a main thread espera ela terminar naturalmente com o join.
    threads.append(source_thread)
    main_logger.info(f"Thread para {SOURCE_CONFIG['source_name']} criada.")
    source_thread.start()
    main_logger.info(f"{SOURCE_CONFIG['source_name']} thread iniciada. O experimento está em andamento.")

    # Espera a thread do Source (que executa o experimento) terminar.
    source_thread.join()

    main_logger.info("--- Simulação PASID-VALIDATOR Concluída ---")
    main_logger.info("Encerrando threads dos LoadBalancers...")
    
    main_logger.info("Programa principal encerrado.")

    print("\n--- Simulação PASID-VALIDATOR Concluída ---")