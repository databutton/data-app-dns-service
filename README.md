# data-app-dns-service

This is a Caddy module that serves as a upstream source for reverse proxies,
specifically designed to be used with Databutton's devx and prodx services.

It retrieves information about available services and their locations from
a Google Firestore database and provides them to the reverse proxy
to be used in forwarding requests.

## Prerequisites

To use this module, you need the following:

- Go version compatible with go.mod version installed on your system.
- The [xcaddy](https://github.com/caddyserver/xcaddy) tool installed.

You can install the xcaddy tool by running the following command:

```
go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
```

and add the $HOME/go/bin directory to your PATH environment variable.

## Usage

To use this module, follow these steps:

1. Clone this repository on your system.
1. In the repository directory, run `make dev` to build the module and
   start Caddy with the custom Caddyfile included in the repository.

## Upgrading packages

Some packages have fragile dependencies.
In case of trouble, try to stick to versions caddy uses here:

```
https://github.com/caddyserver/caddy/blob/master/go.mod
```

What usually works is to rm go.sum, delete dependencies in go.mod and run go get with the packages to install.
E.g. I just ran this and got things working again:

```
go get cloud.google.com/go/firestore@latest github.com/caddyserver/caddy/v2@latest github.com/getsentry/sentry-go@latest go.uber.org/zap@latest google.golang.org/api@latest google.golang.org/grpc@latest
```

You're supposed to be able to run go get -u and/or go mod tidy and get things sorted out but it often doesn't work.

## Sources

pkg/ contains caddy-independent utility packages

caddy-modules/ contains implementations of caddy module interfaces

## Caddy modules

- devxlistener runs a background process listening to firestore

- devxmiddleware does some request processing like setting cors headers,
  by looking at headers and joining with lookups from listener

- devxupstreamer looks at data on the request and returns the upstream address for the reverse proxy
