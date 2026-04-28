package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type ProcessedEvent struct {
	EventID     string `gorm:"primaryKey"`
	ProcessedAt time.Time
}

type PaymentEvent struct {
	EventID   string  `json:"event_id"`
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	Email     string  `json:"customer_email"`
	Status    string  `json:"status"`
	Timestamp int64   `json:"timestamp"`
}

type NotificationService struct {
	db            *gorm.DB
	rabbitConn    *amqp.Connection
	rabbitChannel *amqp.Channel
	processedIDs  map[string]bool
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

	err = ch.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	// Declare DLX
	err = ch.ExchangeDeclare(
		"payment.events.dlx",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare DLX: %v", err)
	}

	// Declare DLQ
	_, err = ch.QueueDeclare(
		"payment.dlq",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare DLQ: %v", err)
	}

	// Bind DLQ to DLX
	err = ch.QueueBind(
		"payment.dlq",
		"",
		"payment.events.dlx",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind DLQ: %v", err)
	}

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

	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	service := &NotificationService{
		db:            db,
		rabbitConn:    conn,
		rabbitChannel: ch,
		processedIDs:  make(map[string]bool),
	}

	service.loadProcessedIDs()

	go func() {
		for msg := range msgs {
			service.handleMessage(msg)
		}
	}()

	log.Println("Notification service started, waiting for messages...")

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

func (s *NotificationService) loadProcessedIDs() {
	var events []ProcessedEvent
	s.db.Find(&events)
	for _, event := range events {
		s.processedIDs[event.EventID] = true
	}
	log.Printf("Loaded %d processed event IDs", len(s.processedIDs))
}

func (s *NotificationService) handleMessage(msg amqp.Delivery) {
	var event PaymentEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		msg.Nack(false, false)
		return
	}

	if s.isDuplicate(event.EventID) {
		log.Printf("Duplicate event %s detected, acknowledging without processing", event.EventID)
		msg.Ack(false)
		return
	}

	// Simulate permanent failure for specific condition to test DLQ (Amount < 0)
	if event.Amount < 0 {
		log.Printf("Simulating failure for EventID %s (Amount < 0) - will requeue for retry", event.EventID)
		time.Sleep(1 * time.Second) // Slow down retries a bit
		msg.Nack(false, true)
		return
	}

	log.Printf("[Notification] Sent email to %s for Order #%s. Amount: $%.2f",
		event.Email, event.OrderID, event.Amount)

	processedEvent := &ProcessedEvent{
		EventID:     event.EventID,
		ProcessedAt: time.Now(),
	}

	if err := s.db.Create(processedEvent).Error; err != nil {
		log.Printf("Failed to record processed event: %v", err)
		msg.Nack(false, true)
		return
	}

	s.processedIDs[event.EventID] = true

	msg.Ack(false)
}

func (s *NotificationService) isDuplicate(eventID string) bool {
	if _, exists := s.processedIDs[eventID]; exists {
		return true
	}

	var count int64
	s.db.Model(&ProcessedEvent{}).Where("event_id = ?", eventID).Count(&count)
	return count > 0
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
