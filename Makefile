all: main

download_rabbitmq:
	sudo apt-get update
	sudo apt-get install rabbitmq-server
	sudo systemctl status rabbitmq-server

rabbit_status: 
	sudo systemctl status rabbitmq-server