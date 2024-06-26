# Trabalho Final de Sistemas Distribuídos
## Aluno: 
Guilherme Diel

## Definição do tema:
Serão criados 3 escalonadores, sendo que 1 será o mestre e os outros 2 ficarão como réplicas para garantir a estabilidade do sistema. Eles estarão constantemente comunicando-se com o servidor mestre, visando à manutenção do serviço. Cada servidor funcionará como host e como escalonador ao mesmo tempo, devido à natureza de sistema distribuído do problema (simulação de vários hosts). O critério de seleção do novo mestre, uma vez constatada a indisponibilidade do anterior, seguirá a lógica de eleição.

### Versões
RabbitMQ: 3.12.1

### Run
`source venv/bin/activate`
`python3 escalonador.py <1..3>`
`python3 job_sender.py <number_of_jobs>`
`python3 election.py`