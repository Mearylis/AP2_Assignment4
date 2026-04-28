FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY . .
RUN go mod tidy
RUN go build -o payment-service .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/payment-service .
EXPOSE 50052
CMD ["./payment-service"]