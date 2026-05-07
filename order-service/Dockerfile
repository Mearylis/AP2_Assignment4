FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY . .
RUN go mod tidy
RUN go build -o order-service .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/order-service .
EXPOSE 50051
CMD ["./order-service"]