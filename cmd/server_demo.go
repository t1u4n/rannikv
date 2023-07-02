package main

import (
	ranniftpb "github.com/TianLuan99/RanniKV.git/internal/grpc"
	"github.com/TianLuan99/RanniKV.git/internal/rannift"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	addr := "localhost:50051"

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	rn, err := rannift.NewRanniNode()
	if err != nil {
		log.Fatalf("failed to initialize RanniNode: %v", err)
	}

	ranniftpb.RegisterRanniftServiceServer(s, rn)
	log.Printf("RanniNode listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
