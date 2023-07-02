package main

import (
	"context"
	"fmt"
	ranniftpb "github.com/TianLuan99/RanniKV.git/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

func main() {
	addr := "localhost:50051"

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := ranniftpb.NewRanniftServiceClient(conn)

	rvResp, err := c.RequestVote(context.TODO(), &ranniftpb.RequestVoteRequest{})
	log.Println(fmt.Sprintf("Request Vote Response: %#v", rvResp))

	aeResp, err := c.AppendEntries(context.TODO(), &ranniftpb.AppendEntriesRequest{})
	log.Println(fmt.Sprintf("Append Entries Response: %#v", aeResp))
}
