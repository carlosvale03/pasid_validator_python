FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copiar o requirements.txt primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instalar dependências do requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código da aplicação para o WORKDIR (/app)
COPY src/ ./src/
COPY main.py ./main.py