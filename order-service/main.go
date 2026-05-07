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

	"encoding/json"
	"pb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
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
	rdb           *redis.Client
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
		s.orderService.invalidateCache(order.ID)
		return nil, status.Errorf(codes.Internal, "payment failed: %v", err)
	}

	if paymentResp.Success {
		s.orderService.db.Model(order).Update("status", "COMPLETED")
		s.orderService.invalidateCache(order.ID)
		return &pb.CreateOrderResponse{
			OrderId:   order.ID,
			Status:    "COMPLETED",
			PaymentId: paymentResp.PaymentId,
		}, nil
	}

	s.orderService.db.Model(order).Update("status", "PAYMENT_FAILED")
	s.orderService.invalidateCache(order.ID)
	return nil, status.Errorf(codes.Internal, "payment processing failed")
}

func (s *OrderServer) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.Order, error) {
	cacheKey := fmt.Sprintf("order:%s", req.OrderId)

	// Cache-aside: Read Path
	val, err := s.orderService.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var order pb.Order
		if err := json.Unmarshal([]byte(val), &order); err == nil {
			log.Printf("Cache hit for order %s", req.OrderId)
			return &order, nil
		}
	}

	log.Printf("Cache miss for order %s", req.OrderId)
	var orderDB Order
	if err := s.orderService.db.First(&orderDB, "id = ?", req.OrderId).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, status.Errorf(codes.NotFound, "order not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get order: %v", err)
	}

	resp := &pb.Order{
		Id:        orderDB.ID,
		UserId:    orderDB.UserID,
		Amount:    orderDB.Amount,
		Status:    orderDB.Status,
		Email:     orderDB.Email,
		CreatedAt: orderDB.CreatedAt.Format(time.RFC3339),
	}

	// Set cache with TTL
	data, _ := json.Marshal(resp)
	ttl := 5 * time.Minute
	s.orderService.rdb.Set(ctx, cacheKey, data, ttl)

	return resp, nil
}

func (s *OrderService) invalidateCache(orderID string) {
	ctx := context.Background()
	cacheKey := fmt.Sprintf("order:%s", orderID)
	s.rdb.Del(ctx, cacheKey)
	log.Printf("Invalidated cache for order %s", orderID)
}

func (s *OrderService) RateLimitInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return handler(ctx, req)
	}

	clientIP := p.Addr.String()
	key := fmt.Sprintf("ratelimit:%s", clientIP)

	// Limit: 10 requests per minute
	limit := 10
	window := time.Minute

	count, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rate limit check failed")
	}

	if count == 1 {
		s.rdb.Expire(ctx, key, window)
	}

	if count > int64(limit) {
		return nil, status.Errorf(codes.ResourceExhausted, "too many requests, please try again later")
	}

	return handler(ctx, req)
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
		db: db,
		rdb: redis.NewClient(&redis.Options{
			Addr: getEnv("REDIS_URL", "localhost:6379"),
		}),
		paymentClient: paymentClient,
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(orderService.RateLimitInterceptor),
	)
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
