# Trabalho T1.3 - DHT 
Implementação de um Serviço de DHT (Tabela Hash Distribuída) usando Pub/Sub e Filas de Mensagem

## Pre-requisitos
```
pip3 install paho-mqtt
```

## Execução
Criação dos nós da DHT. 8 nós devem ser executados para iniciar o recebimento das mensagens.
```
python3 Node.py <nome_do_nó>
```
Em que <nome_do_nó> deve se substituído por um numero ou string. Não é um argumento obrigatório e deve ser único.
