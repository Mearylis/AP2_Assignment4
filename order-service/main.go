package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Order struct {
	ID        string `gorm:"primaryKey"`
	UserID    string
	Amount    float64
	Status    string
	Email     string
	CreatedAt time.Time
}

type OrderService struct {
	db            *gorm.DB
	paymentClient PaymentServiceClient
	grpcServer    *grpc.Server
}

type PaymentRequest struct {
	OrderID string
	Amount  float64
	Email   string
}

type PaymentResponse struct {
	Success   bool
	PaymentID string
}

type PaymentServiceClient interface {
	ProcessPayment(ctx context.Context, req *PaymentRequest) (*PaymentResponse, error)
}

type GRPCPaymentClient struct {
	client PaymentServiceClient
	conn   *grpc.ClientConn
}

func (c *GRPCPaymentClient) ProcessPayment(ctx context.Context, req *PaymentRequest) (*PaymentResponse, error) {
	return c.client.ProcessPayment(ctx, req)
}

type OrderServer struct {
	orderService *OrderService
}

func (s *OrderServer) CreateOrder(ctx context.Context, req *CreateOrderRequest) (*CreateOrderResponse, error) {
	order := &Order{
		ID:        fmt.Sprintf("ORD-%d", time.Now().UnixNano()),
		UserID:    req.UserId,
		Amount:    req.Amount,
		Status:    "PENDING",
		Email:     req.Email,
		CreatedAt: time.Now(),
	}

	if err := s.orderService.db.Create(order).Error; err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create order: %v", err)
	}

	paymentResp, err := s.orderService.paymentClient.ProcessPayment(ctx, &PaymentRequest{
		OrderID: order.ID,
		Amount:  order.Amount,
		Email:   order.Email,
	})

	if err != nil {
		s.orderService.db.Model(order).Update("status", "PAYMENT_FAILED")
		return nil, status.Errorf(codes.Internal, "payment failed: %v", err)
	}

	if paymentResp.Success {
		s.orderService.db.Model(order).Update("status", "COMPLETED")
		return &CreateOrderResponse{
			OrderId:   order.ID,
			Status:    "COMPLETED",
			PaymentId: paymentResp.PaymentID,
		}, nil
	}

	s.orderService.db.Model(order).Update("status", "PAYMENT_FAILED")
	return nil, status.Errorf(codes.Internal, "payment processing failed")
}

func main() {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_USER", "orderuser"),
		getEnv("DB_PASSWORD", "orderpass"),
		getEnv("DB_NAME", "orderdb"),
		getEnv("DB_PORT", "5432"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	db.AutoMigrate(&Order{})

	paymentConn, err := grpc.Dial(
		getEnv("PAYMENT_SERVICE_URL", "localhost:50052"),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect to payment service: %v", err)
	}

	paymentClient := NewPaymentServiceClient(paymentConn)
	grpcPaymentClient := &GRPCPaymentClient{
		client: paymentClient,
		conn:   paymentConn,
	}

	orderService := &OrderService{
		db:            db,
		paymentClient: grpcPaymentClient,
	}

	grpcServer := grpc.NewServer()
	RegisterOrderServiceServer(grpcServer, &OrderServer{orderService: orderService})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		log.Println("Order service starting on port 50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down order service...")

	grpcServer.GracefulStop()
	paymentConn.Close()

	sqlDB, _ := db.DB()
	sqlDB.Close()

	log.Println("Order service stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type CreateOrderRequest struct {
	UserId string
	Amount float64
	Email  string
}

type CreateOrderResponse struct {
	OrderId   string
	Status    string
	PaymentId string
}

type PaymentServiceClient interface {
	ProcessPayment(ctx context.Context, req *PaymentRequest) (*PaymentResponse, error)
}

type PaymentRequest struct {
	OrderId string
	Amount  float64
	Email   string
}

type PaymentResponse struct {
	Success   bool
	PaymentId string
}

func NewPaymentServiceClient(cc *grpc.ClientConn) PaymentServiceClient {
	return &paymentServiceClient{cc}
}

type paymentServiceClient struct {
	cc *grpc.ClientConn
}

func (c *paymentServiceClient) ProcessPayment(ctx context.Context, req *PaymentRequest) (*PaymentResponse, error) {
	return nil, nil
}

func RegisterOrderServiceServer(s *grpc.Server, srv *OrderServer) {
}
