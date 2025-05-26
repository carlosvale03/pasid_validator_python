import logging
import os
from datetime import datetime

LOG_DIR = "logs" # Pasta para os arquivos de log
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Gera um nome de arquivo de log único com base na data e hora atuais
current_time_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILENAME = os.path.join(LOG_DIR, f"experiment_{current_time_str}.log")

# Define o formato do log
log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configura o FileHandler para escrever logs em um arquivo
# Todas as mensagens de DEBUG e acima irão para o arquivo.
file_handler = logging.FileHandler(LOG_FILENAME, mode='w') # 'w' para sobrescrever a cada execução
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.DEBUG)

# Configura o StreamHandler para enviar logs para o console (stderr por padrão)
# Mensagens de INFO e acima irão para o console.
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.INFO)

# Configura o logger raiz para usar os handlers definidos
# Isso garante que qualquer logger criado com logging.getLogger() herde essa configuração.
logging.basicConfig(
    level=logging.DEBUG, # Nível mais baixo a ser capturado pelo logger raiz para o FileHandler
    handlers=[file_handler, console_handler]
)

# Função para obter um logger nomeado
def get_logger(name):
    """Retorna uma instância de logger com o nome fornecido."""
    return logging.getLogger(name)

# Opcional: Adicionar um log inicial para confirmar que o setup foi executado
# logging.getLogger("LoggerSetup").info(f"Logging configurado. Logs serão salvos em: {LOG_FILENAME}")