# Stage 1: Build the Go application
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /producer ./producer

# Stage 2: Create a minimal final image
FROM alpine:latest
WORKDIR /app
COPY --from=builder /producer .
CMD ["./producer"]