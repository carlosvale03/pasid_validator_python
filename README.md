# PASID-VALIDATOR em Python

Este projeto é uma reimplementação em Python do sistema PASID-VALIDATOR, originalmente desenvolvido em Java. O objetivo principal é simular um sistema distribuído cliente-servidor(es) e coletar dados de desempenho, como o Tempo Médio de Resposta (MRT) e tempos intermediários de processamento.

Para a Entrega 01, o foco é ter o código funcional da arquitetura base, com configurações embutidas e sem automações complexas de parâmetros.

## Arquitetura do Sistema

O sistema é composto pelos seguintes componentes principais:

1.  **Source (`source.py`)**:
    * Gera requisições (mensagens) para o sistema.
    * Envia as requisições para o primeiro Load Balancer (LB1).
    * Recebe as mensagens processadas de volta do último componente (Serviço do LB2).
    * Calcula o Tempo Médio de Resposta (MRT) e o Desvio Padrão das respostas.
    * Calcula os tempos intermediários ($T_1$ a $T_5$) com base nos timestamps coletados ao longo do fluxo.
    * Envia mensagens de configuração para um Load Balancer alvo (LB2 neste caso) para variar o número de serviços ativos durante o experimento.

2.  **LoadBalancer (`load_balancer.py`)**:
    * Atua como um middleware, recebendo requisições e distribuindo-as para seus `Service`s internos.
    * Duas instâncias de LoadBalancer são usadas:
        * **LB1**: Recebe requisições do `Source` e as distribui para seus serviços. Os serviços do LB1 enviam as mensagens processadas para o LB2.
        * **LB2**: Recebe requisições dos serviços do LB1 e as distribui para seus próprios serviços. Os serviços do LB2 enviam as mensagens processadas de volta para o `Source`. O LB2 é o alvo das mensagens de configuração do `Source` para variar a quantidade de serviços.
    * Mantém uma fila interna para as requisições.
    * Utiliza uma política de Round Robin (com verificação de disponibilidade) para despachar mensagens para os serviços.
    * Cria e gerencia instâncias de `Service`.

3.  **Service (`service.py`)**:
    * Representa um nó de processamento.
    * Recebe uma mensagem do seu `LoadBalancer` pai.
    * Simula um tempo de processamento (para a Entrega 01, é um `time.sleep()` com variação gaussiana; para a Entrega 02, será um serviço real com IA).
    * Anexa timestamps à mensagem para registrar os tempos de chegada e de processamento.
    * Encaminha a mensagem processada para o próximo destino configurado (outro LoadBalancer ou o Source).

O fluxo principal é:
`Source` → `LB1` → `Serviço(s) do LB1` → `LB2` → `Serviço(s) do LB2` → `Source`

## Estrutura do Projeto

```text
pasid_validator_python/
│
├── src/
│   ├── components/
│   │   ├── __init__.py
│   │   ├── source.py
│   │   ├── load_balancer.py
│   │   └── service.py
│   │
│   └── utils/
│       ├── __init__.py
│       └── logger_setup.py  # Configuração do logging
│
├── main.py                  # Ponto de entrada para executar a simulação
│
├── logs/                    # Diretório para arquivos de log
│   └── experiment_....log   # Arquivo de log detalhado da execução
│
└── README.md
```

## Pré-requisitos

* Python 3.x (desenvolvido com Python 3.12, mas deve funcionar com versões recentes)

## Como Executar

1.  Navegue até o diretório raiz do projeto (`pasid_validator_python/`).
2.  Execute o script `main.py`:
    ```bash
    python main.py
    ```
3.  A saída da simulação (mensagens `INFO` e acima) será exibida no console.
4.  Um arquivo de log detalhado (incluindo mensagens `DEBUG`) será criado na pasta `logs/` com o nome `experiment_YYYY-MM-DD_HH-MM-SS.log`. Este arquivo contém o rastreamento completo do fluxo de mensagens e dos tempos coletados.

## Configuração (Entrega 01)

Para a Entrega 01, todas as configurações (como endereços IP, portas, tempos de serviço, número de mensagens, variação de serviços) estão embutidas diretamente no arquivo `main.py`. Para alterar os parâmetros do experimento, modifique as constantes e dicionários de configuração no início do `main.py` e reexecute o script.

Exemplos de parâmetros configuráveis no `main.py`:
* `SOURCE_LISTEN_PORT`, `LB1_LISTEN_PORT`, `LB2_LISTEN_PORT`
* `SOURCE_CONFIG`: `arrival_delay_ms`, `max_messages_per_cycle`, `qtd_services_variation`, etc.
* `SERVICE_CONFIG_FOR_LB1` e `SERVICE_CONFIG_FOR_LB2`: `time_mean`, `time_std_dev`.
* `LB1_CONFIG` e `LB2_CONFIG`: `max_queue_size`, `initial_num_services`.

## Métricas Coletadas

O sistema coleta e calcula as seguintes métricas principais, que são logadas pelo `Source`:
* **Tempo Médio de Resposta (MRT)**: Calculado para cada ciclo de variação de serviços.
* **Desvio Padrão do MRT**: Calculado para cada ciclo.
* **Tempos Intermediários ($T_1, T_2, T_3, T_4, T_5$)**: Tempos médios para diferentes etapas do processamento, como:
    * $T_1$: Tempo do Source até o LB1.
    * $T_2$: Tempo gasto dentro do LB1.
    * $T_3$: Tempo de processamento no Serviço do LB1.
    * $T_4$: Tempo gasto dentro do LB2.
    * $T_5$: Tempo de processamento no Serviço do LB2.

## Próximas Etapas (Entregas Futuras)

* **Entrega 02 - Experimentos**:
    * Executar os componentes em containers Docker.
    * Substituir a simulação de tempo no `Service` por um serviço real com IA.
    * Gerar gráficos de desempenho (MRT vs. taxa de geração, etc.).
* **Entrega 03 - Artigo**:
    * Escrever um artigo científico detalhando o experimento, a ferramenta e os resultados.
