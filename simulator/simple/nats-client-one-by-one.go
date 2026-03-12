package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type sequentialRequestPayload struct {
	RequestID   int    `json:"request_id"`
	Action      string `json:"action"`
	UserID      int    `json:"user_id"`
	MessageType string `json:"message_type"`
	Timestamp   int64  `json:"timestamp"`
}

var (
	sequentialNATSURL        = flag.String("url", "nats://localhost:4222", "NATS server URL")
	sequentialRequestSubject = flag.String("subject", "tcp.subs.change", "NATS subject to request")
	sequentialRequestTimeout = flag.Duration("timeout", 10*time.Second, "Request timeout")
	sequentialRequestGap     = flag.Duration("gap", 0, "Delay after each response before sending the next request")
	sequentialRequestCount   = flag.Int("count", 0, "Number of requests to send (0 = unlimited)")
)

func main() {
	flag.Parse()

	nc, err := nats.Connect(*sequentialNATSURL)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Println("=== NATS One-by-One Request Client ===")
	fmt.Printf("connected=%s subject=%s timeout=%v gap=%v count=%d\n",
		*sequentialNATSURL, *sequentialRequestSubject, *sequentialRequestTimeout, *sequentialRequestGap, *sequentialRequestCount)

	requestID := 1
	sent := 0

	for {
		if *sequentialRequestCount > 0 && sent >= *sequentialRequestCount {
			fmt.Printf("sent %d requests, exiting\n", sent)
			return
		}

		payload, err := json.Marshal(sequentialRequestPayload{
			RequestID:   requestID,
			Action:      "get_profile",
			UserID:      12345,
			MessageType: "user",
			Timestamp:   time.Now().Unix(),
		})
		if err != nil {
			log.Fatalf("failed to marshal request: %v", err)
		}

		fmt.Printf("\n[%s] sending request #%d subject=%s payload=%s\n",
			time.Now().Format("15:04:05.000"), requestID, *sequentialRequestSubject, string(payload))

		ctx, cancel := context.WithTimeout(context.Background(), *sequentialRequestTimeout)
		msg, err := nc.RequestWithContext(ctx, *sequentialRequestSubject, payload)
		cancel()

		if err != nil {
			fmt.Printf("[%s] request #%d failed: %v\n", time.Now().Format("15:04:05.000"), requestID, err)
		} else {
			fmt.Printf("[%s] response #%d: %s\n", time.Now().Format("15:04:05.000"), requestID, string(msg.Data))
		}

		requestID++
		sent++

		if *sequentialRequestGap > 0 {
			time.Sleep(*sequentialRequestGap)
		}
	}
}
