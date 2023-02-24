.PHONY: build


build:  ## build a custom caddy instance
	(cd run ; xcaddy build --with github.com/lolPants/caddy-requestid --with github.com/databutton/data-app-dns-service=../)


start:
	./run/caddy run --config ./run/Caddyfile


dev:
	make build && make start

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
