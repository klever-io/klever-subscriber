APP_NAME := subscriber
BIN_DIR  := bin

.PHONY: build clean run tidy test

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
