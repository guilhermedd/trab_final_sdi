# Trabalho Final de Sistemas Distribuídos
## Alunos: 
Ana Eloina Nascimento Kraus & Guilherme Diel

## Definição do tema:
Serão criados 3 escalonadores, sendo que 1 será o mestre e os outros 2 ficarão como réplicas para garantir a estabilidade do sistema. Eles estarão constantemente comunicando-se com o servidor mestre, visando à manutenção do serviço. Cada servidor funcionará como host e como escalonador ao mesmo tempo, devido à natureza de sistema distribuído do problema (simulação de vários hosts). O critério de seleção do novo mestre, uma vez constatada a indisponibilidade do anterior, seguirá a lógica do algoritmo do valentão (Bully).

### Versões
RabbitMQ: 3.12.1
