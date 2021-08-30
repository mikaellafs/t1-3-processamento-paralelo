# Trabalho T1.3 - DHT 

**Objetivo**

Experimentar a implementação de sistemas de comunicação indireta por meio de middleware Publish/Subscribe com Filas de Mensagens. Sincronizar a troca de mensagens entre os componentes do sistema. Utilizar brokers de troca de mensagens na implementação de sistemas distribuídos.

**Implementação**

Ao ser inicializado, o nó da DHT deve avisar aos outros por meio do publish de seu nodeID no topic "join", o qual determinará seu intervalo de responsabilidade. Além disso, ele também se inscreve nesse tópico para receber o nodeID dos outros. Ao receber o nodeID de todos os nós (nesse caso 8, inclusive o seu próprio), o nó está pronto para receber mensagens, no entanto, isso não garante que os outros nós também estejam prontos, ou seja, não garante que eles receberam todos os nodeIDs também. O nó agora envia seu nodeID no topic "ack-join" e também se inscreve nele. Quando todos os nodesIDs são recebidos pelo tópico "ack-join" o nó se inscreve nos tópicos put e get, que são os tópicos no qual o cliente irá publicar mensagens.

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
