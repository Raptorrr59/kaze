package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "projects/kaze/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: kazectl <command> [args]")
	}

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewKazeServiceClient(conn)

	switch os.Args[1] {
	case "submit":
		if len(os.Args) < 3 {
			log.Fatalf("Usage: kazectl submit <shell-command>")
		}
		cmd := os.Args[2]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.SubmitJob(ctx, &pb.JobRequest{
			Command: cmd,
		})
		if err != nil {
			log.Fatalf("could not submit job: %v", err)
		}
		log.Printf("Job submitted! ID: %s, Status: %s", resp.JobId, resp.Status)

	case "list-workers":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.ListWorkers(ctx, &pb.ListWorkersRequest{})
		if err != nil {
			log.Fatalf("could not list workers: %v", err)
		}

		log.Printf("Registered Workers (%d):", len(resp.Workers))
		for _, w := range resp.Workers {
			log.Printf("- %s (%s) | Status: %s | CPU: %.1f%% | RAM: %d MB | Last Seen: %s",
				w.WorkerId, w.Hostname, w.Status, w.CpuUsage*100, w.RamUsageBytes/(1024*1024),
				time.Unix(w.LastHeartbeatUnix, 0).Format("15:04:05"))
		}

	default:
		log.Fatalf("Unknown command: %s", os.Args[1])
	}
}
