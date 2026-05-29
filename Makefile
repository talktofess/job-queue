.PHONY: test test-integration demo build vet up down

# Offline: no Postgres, no Docker. The in-memory store + concurrency/chaos tests.
test:
	go vet ./...
	go test ./internal/... ./test/...

# Real Postgres: proves SKIP LOCKED. Needs TEST_DATABASE_URL.
test-integration:
	go test -tags integration ./test/...

demo:
	go run ./cmd/demo

build:
	go build ./...

# Bring up the ops stack (Postgres + worker + Prometheus + Grafana).
up:
	docker compose up --build

down:
	docker compose down -v
