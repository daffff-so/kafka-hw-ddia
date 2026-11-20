.PHONY: up down logs verify ps reset build

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

verify:
	docker compose --profile tools run --rm verify

ps:
	docker compose ps

build:
	docker compose build

reset:
	docker compose down -v

