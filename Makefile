OUTPUT := ./target/release/object-storage-maintenance

# Default target: build the application
all: build

# Build the static binary
build:
	cargo build --release

# Compress the binary with UPX
compress: build
	strip $(OUTPUT)
	upx --brute $(OUTPUT)

# Build and compress for release
release: build compress

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
	@echo "  make compress  Compress the binary with UPX"
	@echo "  make release   Build and compress the binary"
	@echo "  make clean     Remove build artifacts"
