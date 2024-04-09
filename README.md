# data-app-dns-service

This is a Caddy module that serves as a upstream source for reverse proxies,
specifically designed to be used with Databutton's devx and prodx services.

It retrieves information about available services and their locations from
a Google Firestore database and provides them to the reverse proxy
to be used in forwarding requests.

## Prerequisites

To use this module, you need the following:

- Go 1.21 or higher installed on your system.
- The [xcaddy](https://github.com/caddyserver/xcaddy) tool installed.

## Usage

To use this module, follow these steps:

1. Clone this repository on your system.
1. In the repository directory, run `make dev` to build the module and
   start Caddy with the custom Caddyfile included in the repository.
