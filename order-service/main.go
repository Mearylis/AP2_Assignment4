package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
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
	paymentClient pb.PaymentServiceClient
	grpcServer    *grpc.Server
}

type OrderServer struct {
	pb.UnimplementedOrderServiceServer
	orderService *OrderService
}

func (s *OrderServer) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
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

	paymentResp, err := s.orderService.paymentClient.ProcessPayment(ctx, &pb.ProcessPaymentRequest{
		OrderId: order.ID,
		Amount:  order.Amount,
		Email:   order.Email,
	})

	if err != nil {
		s.orderService.db.Model(order).Update("status", "PAYMENT_FAILED")
		return nil, status.Errorf(codes.Internal, "payment failed: %v", err)
	}

	if paymentResp.Success {
		s.orderService.db.Model(order).Update("status", "COMPLETED")
		return &pb.CreateOrderResponse{
			OrderId:   order.ID,
			Status:    "COMPLETED",
			PaymentId: paymentResp.PaymentId,
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
		grpc.WithTimeout(15*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect to payment service: %v", err)
	}

	paymentClient := pb.NewPaymentServiceClient(paymentConn)

	orderService := &OrderService{
		db:            db,
		paymentClient: paymentClient,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrderServiceServer(grpcServer, &OrderServer{orderService: orderService})
	reflection.Register(grpcServer)

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
