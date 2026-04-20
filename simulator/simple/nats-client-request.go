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

type requestPayload struct {
	RequestID   int    `json:"request_id"`
	Action      string `json:"action"`
	UserID      int    `json:"user_id"`
	MessageType string `json:"message_type"`
	Timestamp   int64  `json:"timestamp"`
}

var (
	natsURL         = flag.String("url", "nats://10.255.254.22:30422", "NATS server URL")
	requestSubject  = flag.String("subject", "tcp.subs.change", "NATS subject to request")
	requestInterval = flag.Duration("interval", 3*time.Second, "Interval between NATS requests")
	requestTimeout  = flag.Duration("timeout", 10*time.Second, "Request timeout")
	requestCount    = flag.Int("count", 0, "Number of requests to send (0 = unlimited)")
)

func main() {
	flag.Parse()

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Println("=== NATS Request Client ===")
	fmt.Printf("connected=%s subject=%s interval=%v timeout=%v count=%d\n",
		*natsURL, *requestSubject, *requestInterval, *requestTimeout, *requestCount)

	requestID := 1
	sent := 0

	for {
		if *requestCount > 0 && sent >= *requestCount {
			fmt.Printf("sent %d requests, exiting\n", sent)
			return
		}

		payload, err := json.Marshal(requestPayload{
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
			time.Now().Format("15:04:05.000"), requestID, *requestSubject, string(payload))

		ctx, cancel := context.WithTimeout(context.Background(), *requestTimeout)
		msg, err := nc.RequestWithContext(ctx, *requestSubject, payload)
		cancel()
		if err != nil {
			fmt.Printf("[%s] request #%d failed: %v\n", time.Now().Format("15:04:05.000"), requestID, err)
		} else {
			fmt.Printf("[%s] response #%d: %s\n", time.Now().Format("15:04:05.000"), requestID, string(msg.Data))
		}

		requestID++
		sent++

		if *requestInterval <= 0 {
			return
		}
		time.Sleep(*requestInterval)
	}
}
