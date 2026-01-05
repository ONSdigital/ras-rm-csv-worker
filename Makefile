.PHONY: test
test:
	go test

.PHONY: build
build:
	@if [ "$$(uname -m)" = "aarch64" ] || [ "$$(uname -m)" = "arm64" ]; then \
		GOARCH=arm64; \
	elif [ "$$(uname -m)" = "x86_64" ]; then \
		GOARCH=amd64; \
	else \
		echo "Unsupported architecture: $$(uname -m)"; exit 1; \
	fi; \
	GOOS=linux CGO_ENABLED=0 GOARCH=$$GOARCH go build
