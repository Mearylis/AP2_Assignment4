# Assignment 3 - Event-Driven Architecture with Message Queues

## Idempotency Strategy

The notification service implements idempotency using a combination of in-memory cache and persistent database storage:

1. Each payment event contains a unique `event_id` (format: EVT-{timestamp_nano})
2. Before processing any message, the service checks if this event_id exists in:
    - In-memory map for fast lookup
    - PostgreSQL database for persistence across restarts
3. If the event_id is found, the message is acknowledged but not processed
4. After successful email logging, the event_id is stored in both memory and database
5. This ensures duplicate messages (from at-least-once delivery) don't cause duplicate notifications

## ACK Logic Implementation

Manual acknowledgments are implemented with the following guarantees:

1. **Auto-ACK disabled**: Consumer uses `auto-ack: false` parameter
2. **Message acknowledgment only after**:
    - Successful idempotency check
    - Email logged to console
    - Event ID persisted to database
3. **Negative acknowledgment on failure**:
    - `msg.Nack(false, true)` for transient failures (message requeued)
    - `msg.Nack(false, false)` for permanent failures (message discarded or sent to DLQ)
4. **QoS prefetch**: Set to 1 to process one message at a time

## Dead Letter Queue (DLQ) Implementation

The system implements advanced reliability via a DLQ using RabbitMQ Quorum Queues:
1. **Quorum Queue**: The `payment.completed` queue is initialized as a Quorum Queue (`x-queue-type: quorum`).
2. **Delivery Limit**: The queue leverages `x-delivery-limit: 3`, meaning if a message is rejected (`Nack` with requeue=true) 3 times, it is considered a poison pill.
3. **Dead Letter Exchange**: Upon hitting the delivery limit, the message is automatically routed to the `payment.events.dlx` exchange.
4. **Dead Letter Queue**: Messages are finally stored in `payment.dlq` for administrator review.
*Note: A simulated failure is included in the Notification Service if `Amount < 0` to demonstrate the DLQ behavior.*

## Event Flow

1. Order Service creates order via gRPC
2. Order Service calls Payment Service synchronously
3. Payment Service processes payment and commits to database
4. Payment Service publishes `payment.completed` event to RabbitMQ
5. Notification Service consumes from queue with manual ACKs
6. Notification Service checks idempotency, logs email, stores event ID
7. Message acknowledged only after successful processing

## Running the System

```bash
docker-compose up --build