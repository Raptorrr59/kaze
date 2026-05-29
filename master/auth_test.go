package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func mockTLSContext(cn string) context.Context {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: cn,
		},
	}
	verifiedChains := [][]*x509.Certificate{
		{cert},
	}
	tlsInfo := credentials.TLSInfo{
		State: tls.ConnectionState{
			VerifiedChains: verifiedChains,
		},
	}
	p := &peer.Peer{
		AuthInfo: tlsInfo,
	}
	return peer.NewContext(context.Background(), p)
}

func TestAuthorizeClient_Unauthenticated(t *testing.T) {
	// 1. No peer info in context
	_, err := authorizeClient(context.Background(), "/kaze.KazeService/Heartbeat")
	if err == nil {
		t.Fatalf("expected error without peer info, got nil")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", status.Code(err))
	}
}

func TestAuthorizeClient_WorkerOnlyMethods(t *testing.T) {
	methods := []string{
		"/kaze.KazeService/RegisterWorker",
		"/kaze.KazeService/Heartbeat",
		"/kaze.KazeService/UpdateJobStatus",
		"/kaze.KazeService/StreamLogs",
		"/kaze.KazeService/DeregisterWorker",
	}

	for _, m := range methods {
		t.Run("worker on "+m, func(t *testing.T) {
			ctx := mockTLSContext("worker")
			_, err := authorizeClient(ctx, m)
			if err != nil {
				t.Fatalf("expected success for worker on %s, got error: %v", m, err)
			}
		})

		t.Run("client on "+m, func(t *testing.T) {
			ctx := mockTLSContext("client")
			_, err := authorizeClient(ctx, m)
			if err == nil {
				t.Fatalf("expected permission denied for client on %s, got nil", m)
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("expected PermissionDenied, got %v", status.Code(err))
			}
		})
	}
}

func TestAuthorizeClient_ClientOnlyMethods(t *testing.T) {
	methods := []string{
		"/kaze.KazeService/SubmitJob",
		"/kaze.KazeService/ListWorkers",
		"/kaze.KazeService/WatchLogs",
	}

	for _, m := range methods {
		t.Run("client on "+m, func(t *testing.T) {
			ctx := mockTLSContext("client")
			_, err := authorizeClient(ctx, m)
			if err != nil {
				t.Fatalf("expected success for client on %s, got error: %v", m, err)
			}
		})

		t.Run("worker on "+m, func(t *testing.T) {
			ctx := mockTLSContext("worker")
			_, err := authorizeClient(ctx, m)
			if err == nil {
				t.Fatalf("expected permission denied for worker on %s, got nil", m)
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("expected PermissionDenied, got %v", status.Code(err))
			}
		})
	}
}

func TestAuthorizeClient_SharedMethods(t *testing.T) {
	methods := []string{
		"/kaze.KazeService/ListJobs",
		"/kaze.KazeService/GetJobStatus",
	}

	for _, m := range methods {
		t.Run("client on "+m, func(t *testing.T) {
			ctx := mockTLSContext("client")
			_, err := authorizeClient(ctx, m)
			if err != nil {
				t.Fatalf("expected success for client on %s, got error: %v", m, err)
			}
		})

		t.Run("worker on "+m, func(t *testing.T) {
			ctx := mockTLSContext("worker")
			_, err := authorizeClient(ctx, m)
			if err != nil {
				t.Fatalf("expected success for worker on %s, got error: %v", m, err)
			}
		})

		t.Run("untrusted on "+m, func(t *testing.T) {
			ctx := mockTLSContext("untrusted")
			_, err := authorizeClient(ctx, m)
			if err == nil {
				t.Fatalf("expected permission denied for untrusted on %s, got nil", m)
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("expected PermissionDenied, got %v", status.Code(err))
			}
		})
	}
}

func TestAuthorizeClient_UnknownMethodAndCN(t *testing.T) {
	// Unknown method
	ctx := mockTLSContext("worker")
	_, err := authorizeClient(ctx, "/kaze.KazeService/InvalidMethod")
	if err == nil {
		t.Fatalf("expected error for unknown method, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", status.Code(err))
	}

	// Unknown CN
	ctx = mockTLSContext("untrusted")
	_, err = authorizeClient(ctx, "/kaze.KazeService/Heartbeat")
	if err == nil {
		t.Fatalf("expected error for untrusted CN, got nil")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", status.Code(err))
	}
}
