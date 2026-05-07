package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type ProcessedEvent struct {
	EventID     string `gorm:"primaryKey"`
	ProcessedAt time.Time
}

type PaymentEvent struct {
	EventID   string  `json:"event_id"`
	PaymentID string  `json:"payment_id"`
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	Email     string  `json:"customer_email"`
	Status    string  `json:"status"`
	Timestamp int64   `json:"timestamp"`
}

type EmailSender interface {
	Send(ctx context.Context, email, subject, body string) error
}

type MockEmailSender struct{}

func (m *MockEmailSender) Send(ctx context.Context, email, subject, body string) error {
	// Simulate real-world conditions: network latency
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)

	// Simulate occasional random failures (30% chance)
	if rand.Intn(100) < 30 {
		return fmt.Errorf("temporary network error (simulated)")
	}

	log.Printf("[MockEmail] Sent to %s: %s", email, subject)
	return nil
}

type NotificationService struct {
	db            *gorm.DB
	rdb           *redis.Client
	rabbitConn    *amqp.Connection
	rabbitChannel *amqp.Channel
	emailSender   EmailSender
}

func main() {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_USER", "notifuser"),
		getEnv("DB_PASSWORD", "notifpass"),
		getEnv("DB_NAME", "notifdb"),
		getEnv("DB_PORT", "5432"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	db.AutoMigrate(&ProcessedEvent{})

	rabbitURL := getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	// Declare DLX and DLQ
	ch.ExchangeDeclare("payment.events.dlx", "topic", true, false, false, false, nil)
	ch.QueueDeclare("payment.dlq", true, false, false, false, nil)
	ch.QueueBind("payment.dlq", "#", "payment.events.dlx", false, nil)

	queue, err := ch.QueueDeclare(
		"payment.completed",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type":           "quorum",
			"x-dead-letter-exchange": "payment.events.dlx",
			"x-delivery-limit":       3,
		},
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	var emailProvider EmailSender
	if getEnv("PROVIDER_MODE", "SIMULATED") == "SIMULATED" {
		emailProvider = &MockEmailSender{}
	} else {
		emailProvider = &MockEmailSender{}
	}

	service := &NotificationService{
		db: db,
		rdb: redis.NewClient(&redis.Options{
			Addr: getEnv("REDIS_URL", "localhost:6379"),
		}),
		rabbitConn:    conn,
		rabbitChannel: ch,
		emailSender:   emailProvider,
	}

	go func() {
		for msg := range msgs {
			go service.handleMessage(msg)
		}
	}()

	log.Println("Notification service started (Robust Worker), waiting for messages...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down notification service...")
	ch.Close()
	conn.Close()
	sqlDB, _ := db.DB()
	sqlDB.Close()
	log.Println("Notification service stopped")
}

func (s *NotificationService) handleMessage(msg amqp.Delivery) {
	var event PaymentEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		msg.Nack(false, false)
		return
	}

	ctx := context.Background()
	idempotencyKey := fmt.Sprintf("payment_processed:%s", event.PaymentID)

	// Idempotency check
	success, err := s.rdb.SetNX(ctx, idempotencyKey, "processing", 10*time.Minute).Result()
	if err != nil {
		log.Printf("Failed to check idempotency in Redis: %v", err)
		msg.Nack(false, true)
		return
	}

	if !success {
		status, _ := s.rdb.Get(ctx, idempotencyKey).Result()
		if status == "completed" {
			log.Printf("Payment %s already processed, acknowledging", event.PaymentID)
			msg.Ack(false)
			return
		}
		log.Printf("Payment %s is currently being processed or retry pending", event.PaymentID)
		msg.Nack(false, true)
		return
	}

	// Exponential Backoff Retries
	maxRetries, _ := strconv.Atoi(getEnv("MAX_RETRIES", "5"))
	initialBackoff, _ := strconv.Atoi(getEnv("INITIAL_BACKOFF_SECONDS", "2"))

	subject := fmt.Sprintf("Order #%s Confirmation", event.OrderID)
	body := fmt.Sprintf("Thank you for your payment of $%.2f", event.Amount)

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		lastErr = s.emailSender.Send(ctx, event.Email, subject, body)
		if lastErr == nil {
			break
		}

		backoff := time.Duration(initialBackoff*(1<<i)) * time.Second
		log.Printf("Retry %d/%d for %s in %v: %v", i+1, maxRetries, event.PaymentID, backoff, lastErr)
		time.Sleep(backoff)
	}

	if lastErr != nil {
		log.Printf("All retries failed for payment %s: %v", event.PaymentID, lastErr)
		s.rdb.Del(ctx, idempotencyKey)
		msg.Nack(false, false) // Move to DLQ
		return
	}

	// Mark as completed
	s.rdb.Set(ctx, idempotencyKey, "completed", 24*time.Hour)
	s.db.Create(&ProcessedEvent{
		EventID:     event.EventID,
		ProcessedAt: time.Now(),
	})

	log.Printf("Notification sent successfully for payment %s", event.PaymentID)
	msg.Ack(false)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
