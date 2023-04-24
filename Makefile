run_dev:
	sudo docker-compose -f compose/docker-compose.dev.yml up -d --build

down_dev:
	sudo docker-compose -f compose/docker-compose.dev.yml down

logs_dev:
	sudo docker-compose -f compose/docker-compose.dev.yml logs -f

run_prod:
	sudo docker-compose -f compose/docker-compose.prod.yml up -d --build

down_prod:
	sudo docker-compose -f compose/docker-compose.prod.yml down

logs_prod:
	sudo docker-compose -f compose/docker-compose.prod.yml logs -f

