FROM rust:latest AS build-image
LABEL authors="Bixority SIA"

ARG upx_version=5.0.0
ARG TARGETARCH=${TARGETARCH:-amd64}

WORKDIR /build
ENV CGO_ENABLED=0

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt update && apt install -y --no-install-recommends xz-utils musl-tools musl-dev && \
  curl -Ls https://github.com/upx/upx/releases/download/v${upx_version}/upx-${upx_version}-${TARGETARCH}_linux.tar.xz -o - | tar xvJf - -C /tmp && \
  cp /tmp/upx-${upx_version}-${TARGETARCH}_linux/upx /usr/local/bin/ && \
  chmod +x /usr/local/bin/upx && \
  apt remove -y xz-utils && \
  rm -rf /var/lib/apt/lists/*

COPY ./ /build/
RUN make release

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.description="Object storage maintenance tool"
LABEL authors="Bixority SIA"


WORKDIR /
COPY --from=build-image /build/target/x86_64-unknown-linux-musl/release/object-storage-maintenance /build/LICENSE /

USER nonroot:nonroot

ENTRYPOINT ["/object-storage-maintenance"]
