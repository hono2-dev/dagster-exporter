FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -mod=vendor -o dagster-exporter .

FROM scratch
COPY --from=builder /app/dagster-exporter /dagster-exporter
EXPOSE 8000
ENTRYPOINT ["/dagster-exporter"]
