package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "submit":
		submitCmd := flag.NewFlagSet("submit", flag.ExitOnError)
		image := submitCmd.String("image", "", "Docker image to run")
		cronSpec := submitCmd.String("cron", "", "Cron schedule (e.g. '*/5 * * * *')")
		priority := submitCmd.Int("priority", 0, "Job priority")
		retryLimit := submitCmd.Int("retries", 3, "Retry limit")
		submitCmd.Parse(args)

		if submitCmd.NArg() < 1 {
			log.Fatalf("Usage: kazectl submit [--image <img>] [--cron <spec>] <command>")
		}
		cmdStr := submitCmd.Arg(0)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.SubmitJob(ctx, &pb.JobRequest{
			Command:    cmdStr,
			Image:      *image,
			CronSpec:   *cronSpec,
			Priority:   int32(*priority),
			RetryLimit: int32(*retryLimit),
		})
		if err != nil {
			log.Fatalf("could not submit job: %v", err)
		}
		log.Printf("Job submitted! ID: %s, Status: %s", resp.JobId, resp.Status)

	case "list-jobs":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		resp, err := client.ListJobs(ctx, &pb.ListJobsRequest{})
		if err != nil {
			log.Fatalf("could not list jobs: %v", err)
		}

		log.Printf("Jobs (%d):", len(resp.Jobs))
		for _, j := range resp.Jobs {
			info := ""
			if j.CronSpec != "" {
				info += " [CRON: " + j.CronSpec + "]"
			}
			if j.Image != "" {
				info += " [IMAGE: " + j.Image + "]"
			}
			log.Printf("- %s | %s | Status: %s%s", j.JobId, j.Command, j.Status, info)
		}

	case "status":
		if len(args) < 1 {
			log.Fatalf("Usage: kazectl status <job-id>")
		}
		jobID := args[0]
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		j, err := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: jobID})
		if err != nil {
			log.Fatalf("could not get job status: %v", err)
		}

		log.Printf("Job: %s", j.JobId)
		log.Printf("Command: %s", j.Command)
		if j.Image != "" {
			log.Printf("Image: %s", j.Image)
		}
		if j.CronSpec != "" {
			log.Printf("Cron: %s", j.CronSpec)
		}
		log.Printf("Status: %s", j.Status)
		if j.Result != "" {
			log.Printf("Result:\n%s", j.Result)
		}

	case "logs":
		logsCmd := flag.NewFlagSet("logs", flag.ExitOnError)
		follow := logsCmd.Bool("f", false, "Follow log output")
		logsCmd.Parse(args)

		if logsCmd.NArg() < 1 {
			log.Fatalf("Usage: kazectl logs [-f] <job-id>")
		}
		jobID := logsCmd.Arg(0)

		if *follow {
			stream, err := client.WatchLogs(context.Background(), &pb.WatchLogsRequest{JobId: jobID})
			if err != nil {
				log.Fatalf("could not watch logs: %v", err)
			}
			log.Printf("Watching logs for job %s...", jobID)
			for {
				frame, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatalf("error receiving log frame: %v", err)
				}
				fmt.Print(frame.Data)
			}
		} else {
			j, err := client.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: jobID})
			if err != nil {
				log.Fatalf("could not get job status: %v", err)
			}
			if j.Result != "" {
				fmt.Print(j.Result)
			} else {
				log.Printf("Job %s has no logs yet (Status: %s)", jobID, j.Status)
			}
		}

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
		log.Fatalf("Unknown command: %s", command)
	}
}
