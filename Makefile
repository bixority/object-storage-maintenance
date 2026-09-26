RUST_TARGETARCH ?= x86_64
TARGET_DIR := ./target
OUTPUT := $(TARGET_DIR)/$(RUST_TARGETARCH)-unknown-linux-musl/release/object-storage-maintenance

# Default target: build the application
all: build

# Build the static binary
build:
	rustup target add $(RUST_TARGETARCH)-unknown-linux-musl
	RUSTFLAGS='-C relocation-model=static -C strip=symbols' cargo build --release --target $(RUST_TARGETARCH)-unknown-linux-musl --target-dir $(TARGET_DIR)

# Build for release
release: build
	cp $(OUTPUT) $(TARGET_DIR)/

# Run the application
run: build
	$(OUTPUT)

clean:
	cargo clean

# Display help
help:
	@echo "Makefile commands:"
	@echo "  make           Build the static binary"
	@echo "  make build     Build the static binary"
	@echo "  make release   Build the binary"
	@echo "  make clean     Remove build artifacts"
