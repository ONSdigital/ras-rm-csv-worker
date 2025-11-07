.PHONY: test
test:
	go test

.PHONY: build
build:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build

