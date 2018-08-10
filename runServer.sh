#!/bin/bash
go run server.go -brokers=localhost:9093 -clientCert ~/docker/certs/client.cer.pem -clientKey ~/docker/certs/client.key.pem  -ca ~/docker/certs/server.cer.pem -verbose true -verifySsl true

