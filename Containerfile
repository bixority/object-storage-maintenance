FROM --platform=$TARGETOS/$TARGETARCH rust:1.98.1-slim-trixie AS build-image
LABEL org.opencontainers.image.description="Object storage maintenance tool"
LABEL authors="Bixority SIA"

ARG TARGETARCH
ARG TARGETOS

WORKDIR /build

RUN apt update && apt install -y --no-install-recommends make curl musl-tools musl-dev

COPY ./ /build/

# Map Docker architecture to Rust target
RUN echo "Target architecture is: ${TARGETARCH}" && \
    if [ "${TARGETARCH}" = "amd64" ]; then \
        RUST_TARGETARCH=x86_64 make release; \
    elif [ "${TARGETARCH}" = "arm64" ]; then \
        RUST_TARGETARCH=aarch64 make release; \
    else \
        echo "Unsupported architecture: ${TARGETARCH}"; exit 1; \
    fi

FROM --platform=$TARGETOS/$TARGETARCH gcr.io/distroless/static-debian13:nonroot

LABEL org.opencontainers.image.description="Object storage maintenance tool"
LABEL authors="Bixority SIA"

ARG TARGETARCH
ARG TARGETOS

WORKDIR /
COPY --from=build-image /build/target/object-storage-maintenance /build/LICENSE /

USER nonroot:nonroot

ENTRYPOINT ["/object-storage-maintenance"]
