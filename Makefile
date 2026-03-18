.PHONY: build up up-d up-build up-d-build down logs ps restart rebuild

build:
	docker compose build

up:
	docker compose up

up-d:
	docker compose up -d

up-build:
	docker compose up --build

up-d-build:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

restart:
	docker compose restart

rebuild:
	docker compose build --no-cache