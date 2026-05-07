# Assignment 4 - Performance Optimization & External Integrations

## Caching Strategy (Order Service)
The Order Service implements a **Cache-aside** pattern using Redis:
- **Read Path**: When `GetOrder` is called, the service first checks Redis for the key `order:{id}`.
- **Cache Hit**: Returns the cached order data immediately.
- **Cache Miss**: Queries the PostgreSQL database, populates Redis with a **5-minute TTL**, and returns the data.
- **Invalidation**: Whenever an order is updated (e.g., during `CreateOrder` when status changes from PENDING to COMPLETED/FAILED), the corresponding cache key is deleted (`s.rdb.Del`) to ensure data consistency.

## Robust Background Worker (Notification Service)
The Notification Service has been transformed into a resilient worker:
- **Idempotency**: Uses Redis `SETNX` with the `payment_id` to ensure each payment is notified exactly once, even across multiple worker instances or retries.
- **Adapter Pattern**: Decoupled notification logic using an `EmailSender` interface. The `MockEmailSender` simulates real-world conditions like network latency and random transient failures.
- **Exponential Backoff**: If the notification provider fails, the worker retries the operation with increasing wait times (2s, 4s, 8s, 16s, 32s).
- **Asynchronous Processing**: Messages are processed in separate goroutines to prevent a slow external integration from blocking the message consumer.

## API Rate Limiting (Bonus)
A gRPC interceptor (middleware) is implemented in the Order Service:
- **Mechanism**: Tracks the number of requests per client IP in Redis.
- **Limit**: 10 requests per minute.
- **Behavior**: Returns `HTTP 429 / Codes.ResourceExhausted` when the limit is exceeded.

## Event Flow
1. **Order Service**: Creates order and calls Payment Service synchronously.
2. **Payment Service**: Processes payment and publishes a `PaymentEvent` containing `payment_id` and `event_id` to RabbitMQ.
3. **Notification Service**: 
   - Consumes the event.
   - Checks Redis for idempotency (`payment_processed:{payment_id}`).
   - Attempts to send email via `EmailSender` adapter.
   - Retries with exponential backoff on failure.
   - Acknowledges message only after success or exhaustion of retries (DLQ).

## Setup & Configuration
All configurations are managed via `.env`:
- `REDIS_URL`: Redis connection string.
- `CACHE_TTL_SECONDS`: Order cache expiration.
- `PROVIDER_MODE`: Choose between `SIMULATED` or `REAL` integration.
- `MAX_RETRIES` & `INITIAL_BACKOFF_SECONDS`: Tuning the retry policy.

```bash
# Start the system
docker-compose up --build
```