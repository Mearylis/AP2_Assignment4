package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pb"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Payment struct {
	ID        string `gorm:"primaryKey"`
	OrderID   string `gorm:"uniqueIndex"`
	Amount    float64
	Status    string
	Email     string
	CreatedAt time.Time
}

type PaymentEvent struct {
	EventID   string  `json:"event_id"`
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	Email     string  `json:"customer_email"`
	Status    string  `json:"status"`
	Timestamp int64   `json:"timestamp"`
}

type PaymentService struct {
	db            *gorm.DB
	rabbitConn    *amqp.Connection
	rabbitChannel *amqp.Channel
	queue         amqp.Queue
	grpcServer    *grpc.Server
}

type PaymentServer struct {
	pb.UnimplementedPaymentServiceServer
	paymentService *PaymentService
}

func (s *PaymentServer) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	payment := &Payment{
		ID:        fmt.Sprintf("PAY-%d", time.Now().UnixNano()),
		OrderID:   req.OrderId,
		Amount:    req.Amount,
		Status:    "PROCESSING",
		Email:     req.Email,
		CreatedAt: time.Now(),
	}

	if err := s.paymentService.db.Create(payment).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create payment: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	payment.Status = "COMPLETED"
	s.paymentService.db.Model(payment).Update("status", "COMPLETED")

	event := PaymentEvent{
		EventID:   fmt.Sprintf("EVT-%d", time.Now().UnixNano()),
		OrderID:   payment.OrderID,
		Amount:    payment.Amount,
		Email:     payment.Email,
		Status:    "COMPLETED",
		Timestamp: time.Now().Unix(),
	}

	if err := s.paymentService.publishEvent(event); err != nil {
		log.Printf("Failed to publish event: %v", err)
		return &pb.ProcessPaymentResponse{
			Success:   true,
			PaymentId: payment.ID,
		}, nil
	}

	return &pb.ProcessPaymentResponse{
		Success:   true,
		PaymentId: payment.ID,
	}, nil
}

func (s *PaymentService) publishEvent(event PaymentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = s.rabbitChannel.PublishWithContext(ctx,
		"",
		s.queue.Name,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			MessageId:    event.EventID,
			Timestamp:    time.Now(),
		})

	return err
}

func main() {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_USER", "paymentuser"),
		getEnv("DB_PASSWORD", "paymentpass"),
		getEnv("DB_NAME", "paymentdb"),
		getEnv("DB_PORT", "5432"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	db.AutoMigrate(&Payment{})

	rabbitURL := getEnv("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	err = ch.ExchangeDeclare(
		"payment.events",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
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

	err = ch.QueueBind(
		queue.Name,
		"payment.completed",
		"payment.events",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}

	paymentService := &PaymentService{
		db:            db,
		rabbitConn:    conn,
		rabbitChannel: ch,
		queue:         queue,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, &PaymentServer{paymentService: paymentService})
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		log.Println("Payment service starting on port 50052")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down payment service...")

	grpcServer.GracefulStop()
	ch.Close()
	conn.Close()

	sqlDB, _ := db.DB()
	sqlDB.Close()

	log.Println("Payment service stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
