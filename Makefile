.PHONY: format lint test build run

format:  ## Format source code
	go fmt
	# caddy fmt --overwrite run/Caddyfile

lint:  ## Lint source code
	# TODO: Look into linting options
	go vet

test:  ## Run tests
	go test ./...

build:  ## Build a custom caddy instance
	(cd run ; xcaddy build --with github.com/lolPants/caddy-requestid --with github.com/databutton/data-app-dns-service=../)

start:  ## Start locally built caddy with caddyfile
	./run/caddy run --config ./run/Caddyfile

dev:    ## Build and start
	make build && make start

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
