APP_NAME := subscriber
BIN_DIR  := bin

.PHONY: build clean run tidy test fmt fmt-check vet ci

build:
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/subscriber

clean:
	rm -rf $(BIN_DIR)

run: build
	$(BIN_DIR)/$(APP_NAME)

test:
	go test -race -count=1 ./...

tidy:
	go mod tidy

fmt:
	gofmt -w .

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "files need gofmt:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

vet:
	go vet ./...

# Same gates as .github/workflows/ci.yml so a clean `make ci` predicts a green CI run.
ci: fmt-check vet test
	go build ./...
